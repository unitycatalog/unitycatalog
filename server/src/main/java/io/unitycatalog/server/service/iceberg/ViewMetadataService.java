package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.NormalizedURL;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spark-only view protocol state. SQL and its output schema are supplied by the engine. */
public final class ViewMetadataService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewMetadataService.class);
  private static final String CONTEXT_COUNT = "view.catalogAndNamespace.numParts";
  private static final String CONTEXT_PART = "view.catalogAndNamespace.part.";
  private static final Pattern METADATA_FILE =
      Pattern.compile("[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\\.metadata\\.json");
  private final NormalizedURL root;
  private final FileOperations fileOperations;

  public ViewMetadataService(String root, FileOperations fileOperations) {
    this.fileOperations = fileOperations;
    if (root == null || root.isBlank()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "server.iceberg-view.metadata-root is required when Iceberg views are enabled");
    }
    this.root = metadataRoot(root);
  }

  private static NormalizedURL metadataRoot(String root) {
    try {
      if (Path.of(root).isAbsolute()) {
        return NormalizedURL.from(Path.of(root).normalize().toUri());
      }
      URI uri = URI.create(root);
      if ("file".equals(uri.getScheme()) && uri.getRawAuthority() == null) {
        // Validate escapes before converting a file URI to its decoded local path.
        return NormalizedURL.from(Path.of(NormalizedURL.from(uri).toUri()).toUri());
      }
      if (NormalizedURL.isS3Path(root)
          && "s3".equals(uri.getScheme())
          && uri.getHost() != null
          && uri.getQuery() == null
          && uri.getFragment() == null
          && uri.getRawPath().equals(uri.getPath())
          && uri.equals(uri.normalize())
          && !List.of(uri.getPath().split("/")).contains("..")) {
        return NormalizedURL.from(
            NormalizedURL.from(uri) + "/" + Constants.MANAGED_STORAGE_PREFIX + "/views");
      }
    } catch (IllegalArgumentException | BaseException ignored) {
      // Use the same configuration error for malformed paths and unsupported schemes.
    }
    throw new BaseException(
        ErrorCode.FAILED_PRECONDITION,
        "server.iceberg-view.metadata-root must be an absolute local path, file URI, or s3://"
            + " location");
  }

  public static String sparkSql(ViewVersion version) {
    if (version.representations().size() != 1
        || !(version.representations().get(0) instanceof SQLViewRepresentation sql)
        || !"spark".equals(sql.dialect())
        || sql.sql().isBlank()) {
      throw new BadRequestException(
          "Views require exactly one nonempty SQL representation with dialect spark");
    }
    return sql.sql();
  }

  private static void validateVersion(ViewVersion version) {
    sparkSql(version);
    if (version.defaultNamespace().length() > 1) {
      throw new BadRequestException("Only single-level view namespaces are supported");
    }
  }

  public ViewMetadata create(UUID viewId, CreateViewRequest request) {
    if (request.properties().keySet().stream().anyMatch(k -> k.startsWith("view."))) {
      throw new BadRequestException(
          "Spark view.* properties are server-owned on the Iceberg view API");
    }
    validateVersion(request.viewVersion());
    if (request.schema().columns().isEmpty()) {
      throw new BadRequestException("Views require at least one output column");
    }
    if (request.location() != null) {
      throw new BadRequestException("View locations are assigned by the catalog");
    }
    try {
      ViewMetadata metadata =
          ViewMetadata.builder()
              .assignUUID(viewId.toString())
              .setLocation(directory(viewId.toString()).toString())
              .setCurrentVersion(request.viewVersion(), request.schema())
              .setProperties(request.properties())
              .build();
      return write(metadata);
    } catch (UnsupportedOperationException | ValidationException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, e.getMessage(), e);
    }
  }

  public static Map<String, String> nativeProperties(CreateViewRequest request, String catalog) {
    Map<String, String> properties = new HashMap<>(request.properties());
    ViewVersion version = request.viewVersion();
    properties.put(CONTEXT_COUNT, Integer.toString(1 + version.defaultNamespace().length()));
    properties.put(
        CONTEXT_PART + "0", version.defaultCatalog() != null ? version.defaultCatalog() : catalog);
    for (int i = 0; i < version.defaultNamespace().length(); i++) {
      properties.put(CONTEXT_PART + (i + 1), version.defaultNamespace().level(i));
    }
    // Spark sends SELECT output names separately from the (possibly aliased) view schema.
    String queryNames = request.properties().get("spark.query-column-names");
    List<String> names =
        queryNames == null
            ? request.schema().columns().stream().map(Types.NestedField::name).toList()
            : List.of(queryNames.split(",", -1));
    if (names.size() != request.schema().columns().size()
        || names.stream().anyMatch(String::isEmpty)) {
      throw new BadRequestException(
          "spark.query-column-names must contain one nonempty name per view column. Use a SELECT"
              + " alias for output names containing commas.");
    }
    properties.put("view.query.out.numCols", Integer.toString(names.size()));
    for (int i = 0; i < names.size(); i++) {
      properties.put("view.query.out.col." + i, names.get(i));
    }
    return properties;
  }

  private NormalizedURL directory(String uuid) {
    return NormalizedURL.from(root + "/" + UUID.fromString(uuid));
  }

  private ViewMetadata write(ViewMetadata metadata) {
    NormalizedURL directory = directory(metadata.uuid());
    String file = directory + "/" + UUID.randomUUID() + ".metadata.json";
    try (FileIO io = fileOperations.getFileIO(directory, CredentialContext.READ_WRITE)) {
      ViewMetadataParser.write(metadata, io.newOutputFile(file));
      return readMetadata(io, file);
    } catch (IOException | RuntimeException e) {
      throw storageFailure("Could not persist view metadata", e);
    }
  }

  private void checkMetadataFile(NormalizedURL directory, String file) {
    if (file != null && file.startsWith("file:/") && !file.startsWith("file://")) {
      file = "file://" + file.substring("file:".length());
    }
    String prefix = directory + "/";
    if (file == null
        || !file.startsWith(prefix)
        || !METADATA_FILE.matcher(file.substring(prefix.length())).matches()) {
      throw new BaseException(ErrorCode.INTERNAL, "Invalid persisted view metadata location");
    }
    if ("file".equals(directory.toUri().getScheme())
        && (Files.isSymbolicLink(Path.of(directory.toUri()))
            || !Files.isRegularFile(Path.of(URI.create(file)), LinkOption.NOFOLLOW_LINKS))) {
      throw new BaseException(ErrorCode.INTERNAL, "Invalid persisted view metadata location");
    }
  }

  /** Best-effort cleanup only for a create the catalog definitively rejected. */
  public void discardUnpublished(ViewMetadata metadata) {
    try {
      NormalizedURL directory = directory(metadata.uuid());
      String file = metadata.metadataFileLocation();
      checkMetadataFile(directory, file);
      try (FileIO io = fileOperations.getFileIO(directory, CredentialContext.READ_WRITE)) {
        io.deleteFile(file);
      }
      if ("file".equals(directory.toUri().getScheme())) {
        Files.delete(Path.of(directory.toUri()));
      }
    } catch (IOException | RuntimeException e) {
      LOGGER.warn(
          "Could not remove unpublished view metadata {}", metadata.metadataFileLocation(), e);
    }
  }

  public ViewMetadata read(String uuid, String pointer) {
    NormalizedURL directory = directory(uuid);
    checkMetadataFile(directory, pointer);
    ViewMetadata metadata;
    try (FileIO io = fileOperations.getFileIO(directory)) {
      metadata = readMetadata(io, pointer);
    } catch (IOException | RuntimeException e) {
      throw storageFailure("Could not read view metadata", e);
    }
    if (!uuid.equals(metadata.uuid())) {
      throw new BaseException(ErrorCode.INTERNAL, "Persisted view metadata identity mismatch");
    }
    // Older local snapshots may include the directory's trailing slash.
    if (!directory.toString().equals(metadata.location())
        && !(directory + "/").equals(metadata.location())) {
      throw new BaseException(ErrorCode.INTERNAL, "Persisted view metadata location mismatch");
    }
    return metadata;
  }

  private static ViewMetadata readMetadata(FileIO io, String location) throws IOException {
    // A local InputFile reports a bare path. Return the URI stored in the catalog to every client.
    try (var stream = io.newInputFile(location).newStream()) {
      return ViewMetadataParser.fromJson(location, IcebergObjectMapper.mapper().readTree(stream));
    }
  }

  private static BaseException storageFailure(String message, Throwable cause) {
    return new BaseException(
        ErrorCode.INTERNAL, message, new ServiceFailureException(cause, "%s", message));
  }
}
