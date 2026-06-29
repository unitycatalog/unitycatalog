package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;

import java.io.FileNotFoundException;
import java.io.IOException;
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

  /** Delete entire directory recursively. Note that currently it does nothing for cloud FS */
  public static void deleteDirectory(NormalizedURL url) throws IOException {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> deleteLocalDirectory(url);
      // Currently we can NOT delete the path in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  private static void deleteLocalDirectory(NormalizedURL url) throws IOException {
    Path dirPath = Paths.get(url.toUri());
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

  /** Create a directory for storage location. Note that currently it does nothing for cloud FS */
  public static void createStorageLocationDir(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> createLocalDirectory(url);
      // Currently we can NOT create the directory in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  private static void createLocalDirectory(NormalizedURL url) {
    Path dirPath = Paths.get(url.toUri());
    // Check if directory already exists
    if (Files.exists(dirPath)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + dirPath);
    }
    // Create the directory
    try {
      Files.createDirectories(dirPath);
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + dirPath, e);
    }
  }
}
