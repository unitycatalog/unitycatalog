package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.spark.UCHadoopConf;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A credential-scope-aware FileSystem that routes each operation to the correct underlying
 * FileSystem based on the path's authority and the credential UID stored in the conf.
 *
 * <p>Registered as {@code fs.<scheme>.impl} for temp-credential schemes. A single instance can
 * serve paths from multiple credential scopes: each operation resolves the delegate from a static
 * cache keyed by {@code "<uid>@<authority>"}, so one real cloud FileSystem (e.g. S3AFileSystem) is
 * created per credential scope rather than one per file.
 */
public class UCFileSystem extends FileSystem {

  static final ConcurrentHashMap<String, FileSystem> CACHE = new ConcurrentHashMap<>();

  private URI uri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = name;
  }

  private FileSystem getDelegate(Path path) throws IOException {
    String authority = path.toUri().getAuthority();
    if (authority == null) authority = uri.getAuthority();

    Configuration conf = getConf();
    String uid = conf.get(UCHadoopConf.UC_CREDENTIALS_UID_KEY);
    if (uid != null) {
      FileSystem cached = CACHE.get(uid + "@" + authority);
      if (cached != null) return cached;
    }

    Configuration withoutImpl = new Configuration(conf);
    withoutImpl.unset("fs." + uri.getScheme() + ".impl");
    URI fsUri = URI.create(uri.getScheme() + "://" + authority);
    FileSystem created = FileSystem.newInstance(fsUri, withoutImpl);

    if (uid == null) return created;

    FileSystem existing = CACHE.putIfAbsent(uid + "@" + authority, created);
    if (existing != null) {
      created.close();
      return existing;
    }
    return created;
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return getDelegate(f).open(f, bufferSize);
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
    return getDelegate(f)
        .create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return getDelegate(f).append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return getDelegate(src).rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return getDelegate(f).delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return getDelegate(f).listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {}

  @Override
  public Path getWorkingDirectory() {
    return new Path(uri.getScheme() + "://" + uri.getAuthority() + "/");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return getDelegate(f).mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return getDelegate(f).getFileStatus(f);
  }

  @Override
  public void close() throws IOException {
    // no-op: cached delegates are shared
  }
}
