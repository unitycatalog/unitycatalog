package io.unitycatalog.server.service.iceberg;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class SimpleLocalFileIO implements FileIO {
  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(localPath(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    try {
      Path localPath = Path.of(localPath(path));
      java.nio.file.Files.createDirectories(localPath.getParent());
      return Files.localOutput(localPath.toString());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create local output file: " + path, e);
    }
  }

  @Override
  public void deleteFile(String path) {
    try {
      java.nio.file.Files.deleteIfExists(Path.of(localPath(path)));
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete local file: " + path, e);
    }
  }

  private static String localPath(String path) {
    if (path.startsWith("file:")) {
      return Path.of(URI.create(path)).toString();
    }
    return path;
  }
}
