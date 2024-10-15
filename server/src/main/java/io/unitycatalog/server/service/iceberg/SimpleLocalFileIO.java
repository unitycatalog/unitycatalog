package io.unitycatalog.server.service.iceberg;

import java.net.URI;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class SimpleLocalFileIO implements FileIO {
  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(Paths.get(URI.create(path).getPath()).toFile());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return Files.localOutput(Paths.get(URI.create(path).getPath()).toFile());
  }

  @SneakyThrows
  @Override
  public void deleteFile(String path) {
    FileUtils.deleteDirectory(Paths.get(URI.create(path).getPath()).toFile());
  }
}
