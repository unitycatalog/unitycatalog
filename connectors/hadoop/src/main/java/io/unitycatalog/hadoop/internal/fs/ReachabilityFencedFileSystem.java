package io.unitycatalog.hadoop.internal.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.Reference;
import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/**
 * A {@link FilterFileSystem} that stays strongly reachable until every call it forwards returns.
 *
 * <p>{@link CredScopedFileSystem} releases a wrapper's lease on its shared delegate when the lease
 * becomes unreachable, and closes an evicted delegate once its last lease is released. A caller
 * such as {@code path.getFileSystem(conf).listStatus(path)} keeps no reference to the wrapper, so
 * without a fence the JIT may treat the lease as unreachable while the forwarded call is still
 * running, and the delegate could be closed under it. Each override here calls the inherited method
 * and then {@link Reference#reachabilityFence(Object)} on {@code this}.
 *
 * <p>Every overridable method that {@link FilterFileSystem} declares is overridden, which {@code
 * ReachabilityFencedFileSystemTest} checks so that a Hadoop upgrade adding one cannot slip through.
 * Objects a call returns, such as streams, iterators and builders, are not covered.
 */
abstract class ReachabilityFencedFileSystem extends FilterFileSystem {

  ReachabilityFencedFileSystem(FileSystem fs) {
    super(fs);
  }

  @Override
  public void access(Path path, FsAction mode)
      throws AccessControlException, FileNotFoundException, IOException {
    try {
      super.access(path, mode);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    try {
      return super.append(f, bufferSize, progress);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataOutputStreamBuilder appendFile(Path path) {
    try {
      return super.appendFile(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    try {
      return super.canonicalizeUri(uri);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected void checkPath(Path path) {
    try {
      super.checkPath(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try {
      super.completeLocalOutput(fsOutputFile, tmpLocalFile);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void concat(Path f, Path[] psrcs) throws IOException {
    try {
      super.concat(f, psrcs);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
      throws IOException {
    try {
      super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    try {
      super.copyFromLocalFile(delSrc, overwrite, src, dst);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try {
      super.copyFromLocalFile(delSrc, src, dst);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try {
      super.copyToLocalFile(delSrc, src, dst);
    } finally {
      Reference.reachabilityFence(this);
    }
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
    try {
      return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      Options.ChecksumOpt checksumOpt)
      throws IOException {
    try {
      return super.create(
          f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataOutputStreamBuilder createFile(Path path) {
    try {
      return super.createFile(path);
    } finally {
      Reference.reachabilityFence(this);
    }
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
    try {
      return super.createNonRecursive(
          f, permission, flags, bufferSize, replication, blockSize, progress);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected PathHandle createPathHandle(FileStatus stat, Options.HandleOpt... opts) {
    try {
      return super.createPathHandle(stat, opts);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    try {
      return super.createSnapshot(path, snapshotName);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws AccessControlException,
          FileAlreadyExistsException,
          FileNotFoundException,
          ParentNotDirectoryException,
          UnsupportedFileSystemException,
          IOException {
    try {
      super.createSymlink(target, link, createParent);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      return super.delete(f, recursive);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    try {
      super.deleteSnapshot(path, snapshotName);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    try {
      return super.getAclStatus(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies() throws IOException {
    try {
      return super.getAllStoragePolicies();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected URI getCanonicalUri() {
    try {
      return super.getCanonicalUri();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    try {
      return super.getChildFileSystems();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Configuration getConf() {
    try {
      return super.getConf();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public long getDefaultBlockSize() {
    try {
      return super.getDefaultBlockSize();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    try {
      return super.getDefaultBlockSize(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public short getDefaultReplication() {
    try {
      return super.getDefaultReplication();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    try {
      return super.getDefaultReplication(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path getEnclosingRoot(Path path) throws IOException {
    try {
      return super.getEnclosingRoot(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    try {
      return super.getFileBlockLocations(file, start, len);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    try {
      return super.getFileChecksum(f, length);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    try {
      return super.getFileChecksum(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(Path f)
      throws AccessControlException,
          FileNotFoundException,
          UnsupportedFileSystemException,
          IOException {
    try {
      return super.getFileLinkStatus(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try {
      return super.getFileStatus(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path getHomeDirectory() {
    try {
      return super.getHomeDirectory();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected Path getInitialWorkingDirectory() {
    try {
      return super.getInitialWorkingDirectory();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    try {
      return super.getLinkTarget(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileSystem getRawFileSystem() {
    try {
      return super.getRawFileSystem();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    try {
      return super.getServerDefaults();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    try {
      return super.getServerDefaults(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    try {
      return super.getStatus(p);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    try {
      return super.getStoragePolicy(src);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path getTrashRoot(Path path) {
    try {
      return super.getTrashRoot(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    try {
      return super.getTrashRoots(allUsers);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public URI getUri() {
    try {
      return super.getUri();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public long getUsed() throws IOException {
    try {
      return super.getUsed();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public long getUsed(Path path) throws IOException {
    try {
      return super.getUsed(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    try {
      return super.getWorkingDirectory();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    try {
      return super.getXAttr(path, name);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    try {
      return super.getXAttrs(path, names);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    try {
      return super.getXAttrs(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean hasPathCapability(Path path, String capability) throws IOException {
    try {
      return super.hasPathCapability(path, capability);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    try {
      super.initialize(name, conf);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    try {
      return super.listCorruptFileBlocks(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter)
      throws FileNotFoundException, IOException {
    try {
      return super.listLocatedStatus(f, filter);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    try {
      return super.listLocatedStatus(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    try {
      return super.listStatus(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f) throws IOException {
    try {
      return super.listStatusIterator(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    try {
      return super.listXAttrs(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path makeQualified(Path path) {
    try {
      return super.makeQualified(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try {
      return super.mkdirs(f, permission);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    try {
      return super.mkdirs(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      super.modifyAclEntries(path, aclSpec);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void msync() throws IOException, UnsupportedOperationException {
    try {
      super.msync();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try {
      return super.open(f, bufferSize);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize) throws IOException {
    try {
      return super.open(fd, bufferSize);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FutureDataInputStreamBuilder openFile(Path path)
      throws IOException, UnsupportedOperationException {
    try {
      return super.openFile(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public FutureDataInputStreamBuilder openFile(PathHandle pathHandle)
      throws IOException, UnsupportedOperationException {
    try {
      return super.openFile(pathHandle);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(
      Path path, OpenFileParameters parameters) throws IOException {
    try {
      return super.openFileWithOptions(path, parameters);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(
      PathHandle pathHandle, OpenFileParameters parameters) throws IOException {
    try {
      return super.openFileWithOptions(pathHandle, parameters);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected FSDataOutputStream primitiveCreate(
      Path f,
      FsPermission absolutePermission,
      EnumSet<CreateFlag> flag,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      Options.ChecksumOpt checksumOpt)
      throws IOException {
    try {
      return super.primitiveCreate(
          f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected boolean primitiveMkdir(Path f, FsPermission abdolutePermission) throws IOException {
    try {
      return super.primitiveMkdir(f, abdolutePermission);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    try {
      super.removeAcl(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      super.removeAclEntries(path, aclSpec);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    try {
      super.removeDefaultAcl(path);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    try {
      super.removeXAttr(path, name);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected void rename(Path src, Path dst, Options.Rename... options) throws IOException {
    try {
      super.rename(src, dst, options);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      return super.rename(src, dst);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
      throws IOException {
    try {
      super.renameSnapshot(path, snapshotOldName, snapshotNewName);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    try {
      return super.resolveLink(f);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    try {
      return super.resolvePath(p);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void satisfyStoragePolicy(Path src) throws IOException {
    try {
      super.satisfyStoragePolicy(src);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      super.setAcl(path, aclSpec);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    try {
      super.setOwner(p, username, groupname);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    try {
      super.setPermission(p, permission);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    try {
      return super.setReplication(src, replication);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    try {
      super.setStoragePolicy(src, policyName);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try {
      super.setTimes(p, mtime, atime);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    try {
      super.setVerifyChecksum(verifyChecksum);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    try {
      super.setWorkingDirectory(newDir);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    try {
      super.setWriteChecksum(writeChecksum);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    try {
      super.setXAttr(path, name, value, flag);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    try {
      super.setXAttr(path, name, value);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    try {
      return super.startLocalOutput(fsOutputFile, tmpLocalFile);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean supportsSymlinks() {
    try {
      return super.supportsSymlinks();
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    try {
      return super.truncate(f, newLength);
    } finally {
      Reference.reachabilityFence(this);
    }
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    try {
      super.unsetStoragePolicy(src);
    } finally {
      Reference.reachabilityFence(this);
    }
  }
}
