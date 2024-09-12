package io.unitycatalog.spark;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

// A wrapper over the local file system to test UC table credentials.
public abstract class CredentialTestFileSystem extends RawLocalFileSystem {
  public static boolean credentialCheckEnabled = true;

  abstract String scheme();

  @Override
  protected void checkPath(Path path) {
    // Do nothing.
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return super.create(toLocalPath(f), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (f.toString().startsWith(scheme())) {
      String s3Prefix = scheme() + "//" + f.toUri().getHost();
      return restorePathInFileStatus(s3Prefix, super.getFileStatus(toLocalPath(f)));
    } else {
      assert f.toString().startsWith("file:");
      return super.getFileStatus(f);
    }
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return super.open(toLocalPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    throw new RuntimeException("implement it when testing s3a");
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    String s3Prefix = scheme() + "//" + f.toUri().getHost();
    FileStatus[] files = super.listStatus(toLocalPath(f));
    FileStatus[] res = new FileStatus[files.length];
    for (int i = 0; i < files.length; i++) {
      res[i] = restorePathInFileStatus(s3Prefix, files[i]);
    }
    return res;
  }

  private FileStatus restorePathInFileStatus(String s3Prefix, FileStatus f) {
    String path = f.getPath().toString().replace("file:", s3Prefix);
    return new FileStatus(
        f.getLen(),
        f.isDirectory(),
        f.getReplication(),
        f.getBlockSize(),
        f.getModificationTime(),
        new Path(path));
  }

  private Path toLocalPath(Path f) {
    checkCredentials(f);
    return new Path(f.toString().replaceAll(scheme() + "//.*?/", "file:///"));
  }

  abstract void checkCredentials(Path f);
}
