package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class FileOperations {

  private final ServerProperties serverProperties;

  public FileOperations(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  private static URI createURI(String uri) {
    if (uri.startsWith("s3://") || uri.startsWith("file:")) {
      return URI.create(uri);
    } else {
      return Paths.get(uri).toUri();
    }
  }

  public void deleteDirectory(String path) {
    URI directoryUri = createURI(path);
    UriUtils.validateURI(directoryUri);
    if (directoryUri.getScheme() == null || directoryUri.getScheme().equals("file")) {
      try {
        deleteLocalDirectory(Paths.get(directoryUri));
      } catch (RuntimeException | IOException e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to delete directory: " + path, e);
      }
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
    }
  }

  public static void deleteLocalDirectory(Path dirPath) throws IOException {
    if (Files.exists(dirPath)) {
      try (Stream<Path> walk = Files.walk(dirPath, FileVisitOption.FOLLOW_LINKS)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + path, e);
                  }
                });
      }
    } else {
      throw new FileNotFoundException("Directory does not exist: " + dirPath);
    }
  }
}
