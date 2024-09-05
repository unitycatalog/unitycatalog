package io.unitycatalog.server.persist.utils.hdfs;

import java.io.IOException;

public interface FileSystemHandler {
  void createDirectory(String path) throws IOException;

  void deleteDirectory(String path, boolean recursive) throws IOException;

  boolean exists(String path) throws IOException;
}
