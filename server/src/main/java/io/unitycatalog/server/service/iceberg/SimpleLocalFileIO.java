package io.unitycatalog.server.service.iceberg;

import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class SimpleLocalFileIO implements FileIO {
  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFile(String path) {
    throw new UnsupportedOperationException();
  }
}
