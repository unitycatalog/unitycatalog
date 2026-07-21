package io.unitycatalog.server.service.iceberg;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
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
    File file = toLocalFile(path);
    File parent = file.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
      throw new UncheckedIOException(
          new IOException("Failed to create parent directories for: " + path));
    }
    return Files.localOutput(file);
  }

  @Override
  public void deleteFile(String path) {
    File file = toLocalFile(path);
    if (file.exists() && !file.delete()) {
      throw new UncheckedIOException(new IOException("Failed to delete: " + path));
    }
  }

  // Locations arrive both as plain paths and as file: URIs (matching what
  // org.apache.iceberg.Files.localInput accepts).
  private static File toLocalFile(String path) {
    if (path.startsWith("file:")) {
      return new File(URI.create(path).getPath());
    }
    return new File(path);
  }
}
