package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataService.class);

  private final FileIOFactory fileIOFactory;
  private final String metadataFolder;
  private final String versionHintFilename;
  private final String metadataFilePattern;

  public MetadataService(FileIOFactory fileIOFactory) {
    this(fileIOFactory, null);
  }

  public MetadataService(FileIOFactory fileIOFactory, ServerProperties serverProperties) {
    this.fileIOFactory = fileIOFactory;
    if (serverProperties != null) {
      this.metadataFolder =
          serverProperties.get(ServerProperties.Property.ICEBERG_METADATA_FOLDER);
      this.versionHintFilename =
          serverProperties.get(ServerProperties.Property.ICEBERG_VERSION_HINT_FILENAME);
      this.metadataFilePattern =
          serverProperties.get(ServerProperties.Property.ICEBERG_METADATA_FILE_PATTERN);
    } else {
      this.metadataFolder = "metadata";
      this.versionHintFilename = "version-hint.text";
      this.metadataFilePattern = "v%d.metadata.json";
    }
  }

  public TableMetadata readTableMetadata(String metadataLocation) {
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(metadataLocation));

    return CompletableFuture.supplyAsync(() -> TableMetadataParser.read(fileIO, metadataLocation))
        .join();
  }

  public String resolveMetadataLocationFromStorage(String storageLocation) {
    // Offload blocking I/O to ForkJoinPool (same pattern as readTableMetadata)
    // to avoid blocking Armeria's event loop thread.
    return CompletableFuture.supplyAsync(
            () -> doResolveMetadataLocation(storageLocation))
        .join();
  }

  private String doResolveMetadataLocation(String storageLocation) {
    String normalizedLocation =
        storageLocation.endsWith("/")
            ? storageLocation.substring(0, storageLocation.length() - 1)
            : storageLocation;
    String hintPath =
        normalizedLocation + "/" + metadataFolder + "/" + versionHintFilename;
    FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(normalizedLocation));

    String versionStr;
    try (InputStream is = fileIO.newInputFile(hintPath).newStream()) {
      versionStr = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
    } catch (Exception e) {
      LOGGER.warn("Could not read version hint at: {}", hintPath, e);
      throw new NoSuchTableException(
          "Could not read version hint at: %s. Cause: %s", hintPath, e.getMessage());
    }

    int version;
    try {
      version = Integer.parseInt(versionStr);
    } catch (NumberFormatException e) {
      throw new NoSuchTableException(
          "Invalid version in %s: '%s'", hintPath, versionStr);
    }

    String metadataFile = String.format(metadataFilePattern, version);
    String metadataLocation = normalizedLocation + "/" + metadataFolder + "/" + metadataFile;
    LOGGER.debug(
        "Resolved Iceberg metadata location from storage: {} -> {}",
        storageLocation,
        metadataLocation);
    return metadataLocation;
  }
}
