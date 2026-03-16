package io.unitycatalog.spark.fs;

import io.unitycatalog.spark.auth.CredPropsUtil;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

/**
 * A Hadoop {@link FileSystem} wrapper that enables multiple credential scopes to coexist within a
 * single Spark session.
 *
 * <h2>Problem</h2>
 *
 * <p>Hadoop's native {@link FileSystem} is designed around a single credential per scheme: its
 * internal cache maps {@code (scheme, authority)} to a shared {@link FileSystem} instance, so all
 * operations on the same URI share the same credential. Unity Catalog, however, vends per-table and
 * per-path temporary credentials, meaning that two different tables backed by the same S3 bucket
 * may require entirely different AWS session tokens at the same time. Simply disabling Hadoop's
 * cache (e.g. {@code fs.s3a.impl.disable.cache=true}) would work functionally but creates a new
 * underlying {@link FileSystem} instance for every file access, quickly exhausting native resources
 * such as S3A connection pools (see <a
 * href="https://github.com/unitycatalog/unitycatalog/issues/1378">issue #1378</a>).
 *
 * <h2>Solution</h2>
 *
 * <p>This class introduces a two-level caching strategy:
 *
 * <ol>
 *   <li><b>Hadoop cache disabled for {@code CredScopedFileSystem} itself.</b> {@link CredPropsUtil}
 *       sets {@code fs.<scheme>.impl.disable.cache=true} so that Hadoop always instantiates a fresh
 *       {@code CredScopedFileSystem} for each file access. Because {@code CredScopedFileSystem} is
 *       a thin, stateless wrapper, this is cheap.
 *   <li><b>Global credential-scoped cache for the real delegate.</b> {@code CredScopedFileSystem}
 *       maintains a static {@link #CACHE} keyed by {@link CredScopedKey}, which encodes the
 *       credential scope (table ID + operation, or path + operation). On each {@link
 *       #initialize(URI, Configuration)} call the key is derived from the Hadoop {@link
 *       Configuration} injected by {@link CredPropsUtil}, and the corresponding real {@link
 *       FileSystem} (e.g. {@code S3AFileSystem}) is looked up or created. Requests that share the
 *       same credential scope therefore reuse the same underlying connection pool, while requests
 *       with different credentials transparently receive their own isolated instance.
 * </ol>
 *
 * <p>All public {@link FileSystem} operations are delegated to the credential-scoped instance,
 * including methods that Hadoop's base class leaves unimplemented (ACLs, XAttrs, snapshots, storage
 * policies, etc.), so callers see a fully functional filesystem regardless of which underlying
 * implementation backs it.
 */
public class CredScopedFileSystem extends FileSystem {

  /**
   * LRU cache of real {@link FileSystem} instances keyed by credential scope. Evicted entries are
   * closed to release connection pools and SDK thread pools (e.g. AWS sdk-ScheduledExecutor
   * threads). The cache is bounded to prevent unbounded growth when many distinct credential scopes
   * are accessed in a long-running session.
   */
  private static final Cache<CredScopedKey, FileSystem> CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(100)
          .<CredScopedKey, FileSystem>removalListener(
              notification -> {
                try {
                  notification.getValue().close();
                } catch (IOException e) {
                  // ignore close failures on eviction
                }
              })
          .build();

  private FileSystem delegate;

  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    CredScopedKey key = CredScopedKey.create(uri, conf);
    try {
      delegate = CACHE.get(key, () -> newFileSystem(uri, conf));
    } catch (ExecutionException e) {
      throw new IOException("Failed to initialize filesystem for key " + key, e.getCause());
    }
  }

  private static FileSystem newFileSystem(URI uri, Configuration conf) {
    try {
      Configuration fsConf = new Configuration(conf);

      // S3: restore impl using the side-channel key saved by CredPropsUtil before it overrode
      // fs.<scheme>.impl with CredScopedFileSystem. The side-channel lets callers (including
      // tests) substitute a custom implementation by setting fs.s3a.impl.original; the fallback
      // is S3AFileSystem because Hadoop does not discover it via the service loader by default.
      fsConf.set(
          "fs.s3.impl",
          fsConf.get("fs.s3.impl.original", "org.apache.hadoop.fs.s3a.S3AFileSystem"));
      fsConf.set(
          "fs.s3a.impl",
          fsConf.get("fs.s3a.impl.original", "org.apache.hadoop.fs.s3a.S3AFileSystem"));
      fsConf.set(
          "fs.AbstractFileSystem.s3.impl",
          fsConf.get("fs.AbstractFileSystem.s3.impl.original", "org.apache.hadoop.fs.s3a.S3A"));
      fsConf.set(
          "fs.AbstractFileSystem.s3a.impl",
          fsConf.get("fs.AbstractFileSystem.s3a.impl.original", "org.apache.hadoop.fs.s3a.S3A"));
      fsConf.set("fs.s3.impl.disable.cache", "true");
      fsConf.set("fs.s3a.impl.disable.cache", "true");

      // GCS: impl does not need to be explicitly restored — GoogleHadoopFileSystem is registered
      // via the Java service loader and Hadoop discovers it automatically from the URI scheme.
      fsConf.unset("fs.gs.impl");
      fsConf.unset("fs.AbstractFileSystem.gs.impl");
      fsConf.set("fs.gs.impl.disable.cache", "true");

      // Azure: same as GCS — AzureBlobFileSystem and SecureAzureBlobFileSystem are registered
      // via the Java service loader, so only the CredScopedFileSystem override needs clearing.
      fsConf.unset("fs.abfs.impl");
      fsConf.unset("fs.abfss.impl");
      fsConf.unset("fs.AbstractFileSystem.abfs.impl");
      fsConf.unset("fs.AbstractFileSystem.abfss.impl");
      fsConf.set("fs.abfs.impl.disable.cache", "true");
      fsConf.set("fs.abfss.impl.disable.cache", "true");

      return FileSystem.get(uri, fsConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getScheme() {
    return delegate.getScheme();
  }

  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  @Override
  public String getCanonicalServiceName() {
    return delegate.getCanonicalServiceName();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return delegate.open(f, bufferSize);
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize) throws IOException {
    return delegate.open(fd, bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return delegate.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return delegate.createNonRecursive(
        f, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return delegate.append(f, bufferSize, progress);
  }

  @Override
  public void concat(final Path trg, final Path[] psrcs) throws IOException {
    delegate.concat(trg, psrcs);
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    return delegate.truncate(f, newLength);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return delegate.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return delegate.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return delegate.listStatus(f);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    return delegate.listCorruptFileBlocks(path);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    delegate.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return delegate.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return delegate.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return delegate.getFileStatus(f);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    return delegate.getFileBlockLocations(file, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    return delegate.getFileBlockLocations(p, start, len);
  }

  @Override
  @Deprecated
  public long getDefaultBlockSize() {
    return delegate.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return delegate.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication() {
    return delegate.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return delegate.getDefaultReplication(path);
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    return delegate.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return delegate.getServerDefaults(p);
  }

  @Override
  public boolean hasPathCapability(Path path, String capability) throws IOException {
    return delegate.hasPathCapability(path, capability);
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return delegate.getDelegationToken(renewer);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
      throws IOException {
    return delegate.addDelegationTokens(renewer, credentials);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return delegate.getContentSummary(f);
  }

  @Override
  public void msync() throws IOException {
    delegate.msync();
  }

  @Override
  public void createSymlink(final Path target, final Path link, final boolean createParent)
      throws IOException {
    delegate.createSymlink(target, link, createParent);
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    return delegate.getLinkTarget(f);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    return delegate.createSnapshot(path, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
      throws IOException {
    delegate.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    delegate.deleteSnapshot(path, snapshotName);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    delegate.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    delegate.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    delegate.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    delegate.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    delegate.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return delegate.getAclStatus(path);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    delegate.setXAttr(path, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return delegate.getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return delegate.getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    return delegate.getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return delegate.listXAttrs(path);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    delegate.removeXAttr(path, name);
  }

  @Override
  public void satisfyStoragePolicy(final Path path) throws IOException {
    delegate.satisfyStoragePolicy(path);
  }

  @Override
  public void setStoragePolicy(final Path src, final String policyName) throws IOException {
    delegate.setStoragePolicy(src, policyName);
  }

  @Override
  public void unsetStoragePolicy(final Path src) throws IOException {
    delegate.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(final Path src) throws IOException {
    return delegate.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies() throws IOException {
    return delegate.getAllStoragePolicies();
  }
}
