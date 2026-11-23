package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ValidationException;
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
  private final Path root;

  public ViewMetadataService(String root) {
    if (root == null || root.isBlank()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "server.iceberg-view.metadata-root is required when Iceberg views are enabled");
    }
    this.root = Path.of(root).toAbsolutePath().normalize();
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
              .setLocation(directory(viewId.toString()).toUri().toString())
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

  private Path directory(String uuid) {
    return root.resolve(UUID.fromString(uuid).toString());
  }

  private ViewMetadata write(ViewMetadata metadata) {
    Path directory = directory(metadata.uuid());
    Path file = directory.resolve(UUID.randomUUID() + ".metadata.json");
    try {
      Files.createDirectories(directory);
      // The root is trusted administrator configuration. Reject links in its descendants.
      if (Files.isSymbolicLink(directory)) {
        throw new IOException("View metadata directory is a symbolic link");
      }
      Files.writeString(
          file,
          ViewMetadataParser.toJson(metadata),
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE);
      return ViewMetadataParser.fromJson(file.toUri().toString(), Files.readString(file));
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Could not persist view metadata", e);
    }
  }

  private Path metadataFile(String uuid, String pointer) {
    Path file = Path.of(java.net.URI.create(pointer)).toAbsolutePath().normalize();
    if (!directory(uuid).equals(file.getParent())
        || Files.isSymbolicLink(file.getParent())
        || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
      throw new BaseException(ErrorCode.INTERNAL, "Invalid persisted view metadata location");
    }
    return file;
  }

  /** Best-effort cleanup only for a create the catalog definitively rejected. */
  public void discardUnpublished(ViewMetadata metadata) {
    try {
      Path file = metadataFile(metadata.uuid(), metadata.metadataFileLocation());
      Files.delete(file);
      Files.delete(file.getParent());
    } catch (IOException | RuntimeException e) {
      LOGGER.warn(
          "Could not remove unpublished view metadata {}", metadata.metadataFileLocation(), e);
    }
  }

  public ViewMetadata read(String uuid, String pointer) {
    Path file = metadataFile(uuid, pointer);
    try {
      ViewMetadata metadata = ViewMetadataParser.fromJson(pointer, Files.readString(file));
      if (!uuid.equals(metadata.uuid())) {
        throw new BaseException(ErrorCode.INTERNAL, "Persisted view metadata identity mismatch");
      }
      return metadata;
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Could not read view metadata", e);
    }
  }
}
