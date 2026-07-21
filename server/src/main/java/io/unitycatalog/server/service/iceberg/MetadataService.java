package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
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

  public TableMetadata readTableMetadata(NormalizedURL metadataLocation) {
    // TODO: cache fileIO
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation)) {
      return TableMetadataParser.read(fileIO, metadataLocation.toString());
    }
  }

  public void writeTableMetadata(TableMetadata tableMetadata, NormalizedURL metadataLocation) {
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation, CredentialContext.READ_WRITE)) {
      TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(metadataLocation.toString()));
    }
  }

  /**
   * For local file locations, pre-creates the table's directory layout so staged creates can write
   * data and manifest files before the first metadata commit. Object stores have no directories.
   */
  public void prepareTableLocation(TableMetadata tableMetadata) {
    NormalizedURL location = NormalizedURL.from(tableMetadata.location());
    createStorageLocationDirIfAbsent(location);
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/metadata"));
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/data"));
  }

  /** Best-effort cleanup of a metadata file that lost a commit race or whose commit failed. */
  public void deleteTableMetadata(NormalizedURL metadataLocation) {
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation, CredentialContext.READ_WRITE)) {
      fileIO.deleteFile(metadataLocation.toString());
    } catch (Exception e) {
      // Orphaned metadata files are harmless; the commit outcome is decided by the catalog.
    }
  }

  /** Builds the location of the next metadata file, following Iceberg's naming scheme. */
  public static NormalizedURL newMetadataLocation(TableMetadata tableMetadata, int version) {
    String metadataDirectory =
        tableMetadata
            .properties()
            .getOrDefault(
                TableProperties.WRITE_METADATA_LOCATION, tableMetadata.location() + "/metadata");
    return NormalizedURL.from(
        String.format("%s/%05d-%s.metadata.json", metadataDirectory, version, UUID.randomUUID()));
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
