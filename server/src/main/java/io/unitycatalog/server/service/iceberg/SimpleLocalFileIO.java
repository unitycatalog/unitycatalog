package io.unitycatalog.server.service.iceberg;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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

  @Override
  public void deleteFile(String path) {
    Path directory = Paths.get(URI.create(path).getPath());

    // Walk through the directory tree and delete files and subdirectories
    try {
      java.nio.file.Files.walkFileTree(
          directory,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              java.nio.file.Files.delete(file); // Delete each file
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              java.nio.file.Files.delete(dir); // Delete directory after its contents are deleted
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
