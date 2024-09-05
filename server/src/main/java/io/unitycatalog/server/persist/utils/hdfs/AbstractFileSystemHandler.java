package io.unitycatalog.server.persist.utils.hdfs;

import java.io.IOException;
import java.net.URI;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

public abstract class AbstractFileSystemHandler implements FileSystemHandler {

  // Method to be implemented by subclasses to provide specific Hadoop configurations
  protected abstract Configuration getHadoopConfiguration();
  protected abstract Logger getLogger();
  protected URI storageRoot;

  protected AbstractFileSystemHandler(String storageRoot) {
    this.storageRoot = URI.create(storageRoot);
  }

  @Override
  public void createDirectory(String pathStr) throws IOException {
    Path path = new Path(pathStr);
    Configuration conf = getHadoopConfiguration();
    try (FileSystem fs = FileSystem.get(storageRoot, conf)) {
      if (!fs.exists(path)) {
        fs.mkdirs(path);
        getLogger().debug("Created directory at path: " + path);
      } else {
        getLogger().debug("Directory already exists at path (skipping creation): " + path);
      }
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error while creating directory at path: " + path, e);
    }
  }

  @Override
  public void deleteDirectory(String pathStr, boolean recursive) throws IOException {
    Path path = new Path(pathStr);
    Configuration conf = getHadoopConfiguration();
    try (FileSystem fs = FileSystem.get(storageRoot, conf)) {
      if (fs.exists(path)) {
        // The recursive flag takes care of deleting all files and subdirectories
        fs.delete(path, recursive);
        getLogger().debug("Deleted directory at path: " + path);
      } else {
        getLogger().debug("Directory not found (skipping deletion): " + path);
      }
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error while deleting directory at path: " + path, e);
    }
  }

  @Override
  public boolean exists(String pathStr) throws IOException {
    Path path = new Path(pathStr);
    Configuration conf = getHadoopConfiguration();
    try (FileSystem fs = FileSystem.get(storageRoot, conf)) {
      return fs.exists(path);
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error while checking existence of path: " + path, e);
    }
  }
}
