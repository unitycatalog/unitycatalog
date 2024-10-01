package io.unitycatalog.server.service.iceberg;

import java.io.IOException;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleLocalFileIO implements FileIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLocalFileIO.class);

  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return Files.localOutput(path);
  }

  @Override
  public void deleteFile(String path) {
    try {
      java.nio.file.Files.delete(java.nio.file.Paths.get(path));
    } catch (IOException e) {
      LOGGER.error("Failed to delete file: {}", path, e);
    }
  }
}
