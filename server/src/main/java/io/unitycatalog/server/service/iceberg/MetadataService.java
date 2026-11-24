package io.unitycatalog.server.service.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.LocalStorageLocationValidator;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads and writes Iceberg table metadata for the Iceberg REST catalog. */
public class MetadataService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataService.class);

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
    try (FileIO fileIO = fileOperations.getFileIO(persistedTableLocation)) {
      tableMetadata = TableMetadataParser.read(fileIO, metadataLocation.toString());
    }
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    return tableMetadata;
  }

  /**
   * Reads existing metadata within an authorized storage boundary. The table directory must be a
   * child of that boundary and contain all referenced files; registration never rewrites them.
   */
  public TableMetadata readRegistrationMetadata(
      NormalizedURL metadataLocation, NormalizedURL storageRoot) {
    return readRegistrationMetadata(metadataLocation, storageRoot, ignored -> {});
  }

  /** Checks the discovered directory before reading retained manifests; persistence rechecks it. */
  public TableMetadata readRegistrationMetadata(
      NormalizedURL metadataLocation,
      NormalizedURL storageRoot,
      Consumer<NormalizedURL> validateTableLocation) {
    validateMetadataLocation(metadataLocation, storageRoot);
    boolean isLocal = UriScheme.fromURI(storageRoot.toUri()) == UriScheme.FILE;
    // Keep local ancestor checks rooted at the authorized boundary. Cloud credentials need only
    // the metadata directory, which also permits an external location at a bucket root.
    NormalizedURL readLocation =
        isLocal
            ? storageRoot
            : NormalizedURL.from(new Path(metadataLocation.toString()).getParent().toString());
    TableMetadata metadata;
    try (FileIO fileIO = fileOperations.getFileIO(readLocation)) {
      metadata = TableMetadataParser.read(fileIO, metadataLocation.toString());
    } catch (NotFoundException
        | UncheckedIOException
        | IllegalArgumentException
        | ValidationException e) {
      throw registrationReadFailure("Cannot read Iceberg metadata from " + metadataLocation, e);
    }
    // REST clients require a table UUID to build commit requirements, including for format v1.
    ValidationUtils.checkArgument(
        metadata.uuid() != null,
        "Iceberg metadata at '%s' must contain table-uuid before registration.",
        metadataLocation);
    NormalizedURL tableLocation =
        registrationLocation(metadata.location(), /* isDirectory= */ true);
    validateMetadataLocation(tableLocation, storageRoot);
    validateMetadataLocation(metadataLocation, tableLocation);
    validateTableMetadataLocation(metadata, tableLocation);
    validateTableLocation.accept(tableLocation);
    return validateRegistrationFiles(
        metadata, tableLocation, isLocal ? storageRoot : tableLocation);
  }

  /** Checks every retained snapshot, including historical data and delete manifests. */
  private TableMetadata validateRegistrationFiles(
      TableMetadata metadata, NormalizedURL tableLocation, NormalizedURL readLocation) {
    for (String property :
        List.of(
            TableProperties.WRITE_METADATA_LOCATION,
            TableProperties.WRITE_DATA_LOCATION,
            TableProperties.WRITE_FOLDER_STORAGE_LOCATION,
            TableProperties.OBJECT_STORE_PATH)) {
      String location = metadata.properties().get(property);
      if (location != null) {
        validateRegistrationLocation(location, tableLocation, /* isDirectory= */ true);
      }
    }
    metadata
        .previousFiles()
        .forEach(
            file ->
                validateRegistrationLocation(file.file(), tableLocation, /* isDirectory= */ false));
    metadata
        .statisticsFiles()
        .forEach(
            file ->
                validateRegistrationLocation(file.path(), tableLocation, /* isDirectory= */ false));
    metadata
        .partitionStatisticsFiles()
        .forEach(
            file ->
                validateRegistrationLocation(file.path(), tableLocation, /* isDirectory= */ false));
    if (metadata.snapshots().isEmpty()) {
      return metadata;
    }
    Set<String> checkedManifests = new HashSet<>();
    try (FileIO fileIO = fileOperations.getFileIO(readLocation)) {
      for (Snapshot snapshot : metadata.snapshots()) {
        if (snapshot.manifestListLocation() != null) {
          validateRegistrationLocation(
              snapshot.manifestListLocation(), tableLocation, /* isDirectory= */ false);
        }
        for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
          validateRegistrationLocation(manifest.path(), tableLocation, /* isDirectory= */ false);
          if (checkedManifests.add(manifest.path())) {
            validateRegistrationManifest(metadata, tableLocation, fileIO, manifest);
          }
        }
      }
      // Legacy snapshots cache lazy manifest lists that capture this FileIO. Detach the response
      // while it is open, so response serialization cannot use a closed storage client.
      return TableMetadataParser.fromJson(
          metadata.metadataFileLocation(), TableMetadataParser.toJson(metadata));
    } catch (IOException
        | NotFoundException
        | UncheckedIOException
        | IllegalArgumentException
        | ValidationException
        | AvroRuntimeException e) {
      throw registrationReadFailure("Cannot read Iceberg manifests for " + tableLocation, e);
    }
  }

  private static BaseException registrationReadFailure(String message, Exception failure) {
    // A parse failure may be wrapped in Iceberg's RuntimeIOException. Preserve transport failures
    // as retryable server errors instead of asking the caller to change valid metadata.
    boolean ioFailure = false;
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
      if (cause instanceof JsonProcessingException
          || cause instanceof EOFException
          || cause instanceof InvalidAvroMagicException
          || cause instanceof FileNotFoundException) {
        return new BaseException(ErrorCode.INVALID_ARGUMENT, message, failure);
      }
      ioFailure |= cause instanceof IOException;
    }
    return ioFailure
        ? new BaseException(ErrorCode.UNAVAILABLE, "Failed to access Iceberg storage.", failure)
        : new BaseException(ErrorCode.INVALID_ARGUMENT, message, failure);
  }

  private static void validateRegistrationManifest(
      TableMetadata metadata, NormalizedURL tableLocation, FileIO fileIO, ManifestFile manifest)
      throws IOException {
    ValidationUtils.checkArgument(
        metadata.specsById().containsKey(manifest.partitionSpecId()),
        "Iceberg manifest '%s' references unknown partition spec %s.",
        manifest.path(),
        manifest.partitionSpecId());
    try (ManifestReader<?> reader =
        switch (manifest.content()) {
          case DATA -> ManifestFiles.read(manifest, fileIO, metadata.specsById());
          case DELETES -> ManifestFiles.readDeleteManifest(manifest, fileIO, metadata.specsById());
        }) {
      reader
          .select(List.of("file_path"))
          .forEach(
              file ->
                  validateRegistrationLocation(
                      file.location(), tableLocation, /* isDirectory= */ false));
    }
  }

  private static void validateRegistrationLocation(
      String path, NormalizedURL tableLocation, boolean isDirectory) {
    NormalizedURL location = registrationLocation(path, isDirectory);
    // Cleanup deletes descendants of the table directory, so only directory properties may equal
    // it.
    ValidationUtils.checkArgument(
        ExternalLocationUtils.stillCovers(tableLocation, location)
            && (isDirectory || !ExternalLocationUtils.stillCovers(location, tableLocation)),
        "Iceberg registration location ('%s') must be %s the table location ('%s').",
        location,
        isDirectory ? "at or below" : "strictly below",
        tableLocation);
  }

  /** Validates the raw registration pointer before normalization can change its object key. */
  public static NormalizedURL registrationMetadataLocation(String path) {
    return registrationLocation(path, /* isDirectory= */ false);
  }

  /** Cloud object keys must retain the spelling that FileIO reads and cleanup deletes. */
  private static NormalizedURL registrationLocation(String path, boolean isDirectory) {
    NormalizedURL location = NormalizedURL.from(path);
    LocalStorageLocationValidator.validateCanonicalLocalLocation(location);
    if (UriScheme.fromURI(location.toUri()) != UriScheme.FILE) {
      ValidationUtils.checkArgument(
          (path.equals(location.toString()) || (isDirectory && path.equals(location + "/")))
              && location.toUri().getRawQuery() == null
              && location.toUri().getRawFragment() == null
              && !path.contains("%"),
          "Iceberg registration location ('%s') must use its canonical cloud object path.",
          path);
    }
    return location;
  }

  /** Writes metadata only within the table location persisted by UC. */
  public void writeTableMetadata(
      TableMetadata tableMetadata,
      NormalizedURL metadataLocation,
      NormalizedURL persistedTableLocation) {
    validateTableMetadataLocation(tableMetadata, persistedTableLocation);
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    try (FileIO fileIO =
        fileOperations.getFileIO(persistedTableLocation, CredentialContext.READ_WRITE)) {
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
    // The table location first: creating a directory at a symbolic link is refused, so a linked
    // table location fails here, before metadata/ or data/ could be created through it.
    createStorageLocationDirIfAbsent(location);
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/metadata"));
    createStorageLocationDirIfAbsent(NormalizedURL.from(location + "/data"));
  }

  /**
   * Best-effort cleanup of a metadata file whose commit was rejected, such as one that lost a
   * commit race, constrained to the persisted table location.
   */
  public void deleteTableMetadata(
      NormalizedURL metadataLocation, NormalizedURL persistedTableLocation) {
    validateMetadataLocation(metadataLocation, persistedTableLocation);
    try (FileIO fileIO =
        fileOperations.getFileIO(persistedTableLocation, CredentialContext.READ_WRITE)) {
      fileIO.deleteFile(metadataLocation.toString());
    } catch (Exception e) {
      // Orphaned metadata files are harmless; the commit outcome is decided by the catalog. Still
      // log at warn so a persistent cleanup failure (e.g. a credential or permission problem) stays
      // diagnosable rather than being silently swallowed.
      LOGGER.warn("Best-effort cleanup of Iceberg metadata file {} failed", metadataLocation, e);
    }
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
        metadataLocation.toUri().getRawQuery() == null
            && metadataLocation.toUri().getRawFragment() == null
            && ExternalLocationUtils.stillCovers(persistedTableLocation, metadataLocation)
            && !ExternalLocationUtils.stillCovers(metadataLocation, persistedTableLocation),
        "Iceberg metadata location ('%s') must be a subpath of the persisted table location"
            + " ('%s').",
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
