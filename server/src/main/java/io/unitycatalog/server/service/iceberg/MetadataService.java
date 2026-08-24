package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import io.unitycatalog.server.utils.ValidationUtils;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;

/** Reads and writes Iceberg table metadata for the Iceberg REST catalog. */
public class MetadataService {

  private static final Pattern METADATA_FILE_VERSION =
      Pattern.compile("^(?:v)?(\\d+)-?.*\\.metadata\\.json(?:\\.gz)?$");

  private final FileOperations fileOperations;

  public MetadataService(FileOperations fileOperations) {
    this.fileOperations = fileOperations;
  }

  /**
   * Reads metadata only after confirming both the requested file and the table root recorded in the
   * metadata are inside the persisted table location.
   */
  public TableMetadata readTableMetadata(
      NormalizedURL metadataLocation, NormalizedURL persistedTableLocation) {
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    // TODO: cache fileIO
    TableMetadata tableMetadata;
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation)) {
      tableMetadata = TableMetadataParser.read(fileIO, metadataLocation.toString());
    }
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    return tableMetadata;
  }

  /** Writes metadata only within the table location persisted by UC. */
  public void writeTableMetadata(
      TableMetadata tableMetadata,
      NormalizedURL metadataLocation,
      NormalizedURL persistedTableLocation) {
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation, CredentialContext.READ_WRITE)) {
      TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(metadataLocation.toString()));
    }
  }

  /**
   * For local file locations, pre-creates the table's directory layout so staged creates can write
   * data and manifest files before the first metadata commit. Object stores have no directories.
   * Prepares the directories only after validating the table's client-supplied locations.
   */
  public void prepareTableLocation(
      TableMetadata tableMetadata, NormalizedURL persistedTableLocation) {
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    NormalizedURL location = persistedTableLocation;
    createStorageLocationDirIfAbsent(location);
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/metadata"));
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/data"));
  }

  /** Best-effort cleanup of a metadata file that lost a commit race or whose commit failed. */
  private void deleteTableMetadata(NormalizedURL metadataLocation) {
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation, CredentialContext.READ_WRITE)) {
      fileIO.deleteFile(metadataLocation.toString());
    } catch (Exception e) {
      // Orphaned metadata files are harmless; the commit outcome is decided by the catalog.
    }
  }

  /** Best-effort cleanup constrained to the persisted table location. */
  public void deleteTableMetadata(
      NormalizedURL metadataLocation, NormalizedURL persistedTableLocation) {
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    deleteTableMetadata(metadataLocation);
  }

  /** Builds the location of the next metadata file, following Iceberg's naming scheme. */
  private static NormalizedURL newMetadataLocation(TableMetadata tableMetadata, int version) {
    String metadataDirectory =
        tableMetadata
            .properties()
            .getOrDefault(
                TableProperties.WRITE_METADATA_LOCATION, tableMetadata.location() + "/metadata");
    return NormalizedURL.from(
        String.format("%s/%05d-%s.metadata.json", metadataDirectory, version, UUID.randomUUID()));
  }

  /** Builds and validates the next metadata location against the persisted table root. */
  public static NormalizedURL newMetadataLocation(
      TableMetadata tableMetadata, int version, NormalizedURL persistedTableLocation) {
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    NormalizedURL metadataLocation = newMetadataLocation(tableMetadata, version);
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    return metadataLocation;
  }

  /**
   * Validates the table root and optional write-metadata directory from an Iceberg metadata
   * document against the location stored in UC's table DAO.
   */
  public static void validateTableMetadataLocation(
      TableMetadata tableMetadata, NormalizedURL persistedTableLocation) {
    NormalizedURL metadataTableLocation = NormalizedURL.from(tableMetadata.location());
    ValidationUtils.checkArgument(
        metadataTableLocation.equals(persistedTableLocation),
        "Iceberg table location ('%s') must match the persisted table location ('%s').",
        metadataTableLocation,
        persistedTableLocation);

    String configuredMetadataLocation =
        tableMetadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);
    if (configuredMetadataLocation != null) {
      validateMetadataLocation(
          NormalizedURL.from(configuredMetadataLocation), persistedTableLocation);
    }
  }

  /** Validates that a metadata file or metadata directory is strictly under the table root. */
  public static void validateMetadataLocation(
      NormalizedURL metadataLocation, NormalizedURL persistedTableLocation) {
    ValidationUtils.checkArgument(
        metadataLocation.toString().startsWith(persistedTableLocation.toString() + "/"),
        "Iceberg metadata location ('%s') must be a subpath of the persisted table location ('%s').",
        metadataLocation,
        persistedTableLocation);
  }

  public static int parseMetadataVersion(NormalizedURL metadataLocation) {
    String location = metadataLocation.toString();
    String fileName = location.substring(location.lastIndexOf('/') + 1);
    Matcher matcher = METADATA_FILE_VERSION.matcher(fileName);
    if (matcher.matches()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        return -1;
      }
    }
    return -1;
  }

  public static String toIcebergMetadataLocation(NormalizedURL metadataLocation) {
    return UriScheme.fromURI(metadataLocation.toUri()) == UriScheme.FILE
        ? Paths.get(metadataLocation.toUri()).toString()
        : metadataLocation.toString();
  }

  private static void createStorageLocationDirIfAbsent(NormalizedURL location) {
    try {
      FileOperations.createStorageLocationDir(location);
    } catch (BaseException e) {
      if (e.getErrorCode() != ErrorCode.ALREADY_EXISTS) {
        throw e;
      }
    }
  }
}
