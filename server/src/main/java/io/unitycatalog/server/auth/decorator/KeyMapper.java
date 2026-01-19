package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.CredentialRepository;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Maps resource identifiers to internal UUIDs for authorization purposes.
 *
 * <p>This class serves as a translation layer in the Unity Catalog permission system, converting
 * human-readable resource identifiers (names, full names, or UUIDs) into the internal UUID format
 * required for authorization checks. It handles the hierarchical nature of Unity Catalog resources
 * by automatically resolving parent resource IDs when needed.
 *
 * <h2>Permission System Integration</h2>
 *
 * <p>In the authorization flow:
 *
 * <ol>
 *   <li>A method annotated with {@code @AuthorizeExpression} specifies required permissions
 *   <li>The UnityAccessDecorator intercepts the call and extracts resource keys from parameters
 *   <li>KeyMapper transforms these resource keys (usually names) into resource IDs (UUIDs)
 *   <li>The evaluator checks if the principal has permission on the resolved resource IDs
 * </ol>
 *
 * <h2>Supported Resource Types</h2>
 *
 * <ul>
 *   <li>METASTORE - Singleton metastore resource
 *   <li>CATALOG - Top-level namespace
 *   <li>SCHEMA - Namespace within a catalog
 *   <li>TABLE - Data table within a schema (supports staging tables too)
 *   <li>VOLUME - Storage volume within a schema
 *   <li>FUNCTION - User-defined function within a schema
 *   <li>REGISTERED_MODEL - ML model within a schema
 *   <li>EXTERNAL_LOCATION - External storage location
 *   <li>CREDENTIAL - Cloud storage credential
 * </ul>
 *
 * <h2>Input Formats</h2>
 *
 * <p>The mapper accepts flexible input formats for resources:
 *
 * <ul>
 *   <li><b>Hierarchical keys:</b> Separate keys for each level (e.g., CATALOG="main",
 *       SCHEMA="sales", TABLE="transactions")
 *   <li><b>Full names:</b> Dot-separated full names (e.g., TABLE="main.sales.transactions")
 *   <li><b>UUIDs:</b> Direct UUID references (e.g., TABLE="550e8400-e29b-41d4-a716-446655440000")
 * </ul>
 *
 * <h2>Hierarchical Resolution</h2>
 *
 * <p>For nested resources (tables, volumes, functions, models), the mapper automatically resolves
 * and populates parent resource IDs. For example, when given just a table name, it returns:
 *
 * <ul>
 *   <li>TABLE → table UUID
 *   <li>SCHEMA → parent schema UUID
 *   <li>CATALOG → parent catalog UUID
 * </ul>
 *
 * <p>This ensures that authorization checks can evaluate permissions at all levels of the
 * hierarchy.
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // Input: Table referenced by full name
 * Map<SecurableType, Object> resourceKeys = Map.of(TABLE, "main.sales.transactions");
 *
 * // Output: Resolved UUIDs for table and its parents
 * Map<SecurableType, Object> resourceIds = keyMapper.mapResourceKeys(resourceKeys);
 * // Returns: {TABLE: <table-uuid>, SCHEMA: <schema-uuid>, CATALOG: <catalog-uuid>}
 * }</pre>
 *
 * <h2>Special Cases</h2>
 *
 * <ul>
 *   <li><b>Credentials:</b> Can be null (when not being updated), a name, or a UUID
 *   <li><b>External Locations:</b> Can be referenced by name or UUID
 *   <li><b>Tables:</b> Supports both regular tables and staging tables via ID lookup
 * </ul>
 *
 * @see UnityAccessDecorator
 * @see io.unitycatalog.server.auth.annotation.AuthorizeExpression
 */
public class KeyMapper {
  private final ExternalLocationUtils externalLocationUtils;
  private final CatalogRepository catalogRepository;
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final VolumeRepository volumeRepository;
  private final FunctionRepository functionRepository;
  private final ModelRepository modelRepository;
  private final MetastoreRepository metastoreRepository;
  private final ExternalLocationRepository externalLocationRepository;
  private final CredentialRepository credentialRepository;

  public KeyMapper(Repositories repositories) {
    this.externalLocationUtils = repositories.getExternalLocationUtils();
    this.catalogRepository = repositories.getCatalogRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.tableRepository = repositories.getTableRepository();
    this.volumeRepository = repositories.getVolumeRepository();
    this.functionRepository = repositories.getFunctionRepository();
    this.modelRepository = repositories.getModelRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.externalLocationRepository = repositories.getExternalLocationRepository();
    this.credentialRepository = repositories.getCredentialRepository();
  }

  public Map<SecurableType, Object> mapResourceKeys(Map<SecurableType, Object> resourceKeys) {
    Map<SecurableType, Object> resourceIds = new HashMap<>();

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(TABLE)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(TABLE);
      TableInfo table = tableRepository.getTable(fullName);
      resourceIds.put(TABLE, UUID.fromString(table.getTableId()));
    }

    // If only TABLE is specified, assuming its value is a full table name (including catalog and
    // schema) or a table UUID.
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(TABLE)) {
      String fullName = (String) resourceKeys.get(TABLE);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      boolean isTableName = fullName.contains(".");

      UUID catalogId;
      UUID schemaId;
      UUID tableId;
      if (isTableName) {
        TableInfo table = tableRepository.getTable(fullName);
        String fullSchemaName = table.getCatalogName() + "." + table.getSchemaName();
        SchemaInfo schema = schemaRepository.getSchema(fullSchemaName);
        CatalogInfo catalog = catalogRepository.getCatalog(table.getCatalogName());
        catalogId = UUID.fromString(catalog.getId());
        schemaId = UUID.fromString(schema.getSchemaId());
        tableId = UUID.fromString(table.getTableId());
      } else {
        // It may be the ID of either a table or staging table.
        tableId = UUID.fromString(fullName);
        Pair<UUID, UUID> catalogAndSchemaIds =
            tableRepository.getCatalogSchemaIdsByTableOrStagingTableId(tableId);
        catalogId = catalogAndSchemaIds.getLeft();
        schemaId = catalogAndSchemaIds.getRight();
      }

      resourceIds.put(TABLE, tableId);
      resourceIds.put(SCHEMA, schemaId);
      resourceIds.put(CATALOG, catalogId);
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(VOLUME)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(VOLUME);
      VolumeInfo volume = volumeRepository.getVolume(fullName);
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
    }

    // If only VOLUME is specified, assuming its value is a full volume name (including catalog and
    // schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(VOLUME)) {
      String fullName = (String) resourceKeys.get(VOLUME);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      VolumeInfo volume =
          (fullName.contains("."))
              ? volumeRepository.getVolume(fullName)
              : volumeRepository.getVolumeById(fullName);
      String fullSchemaName = volume.getCatalogName() + "." + volume.getSchemaName();
      SchemaInfo schema = schemaRepository.getSchema(fullSchemaName);
      CatalogInfo catalog = catalogRepository.getCatalog(volume.getCatalogName());
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(FUNCTION)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(FUNCTION);
      FunctionInfo function = functionRepository.getFunction(fullName);
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
    }

    // If only FUNCTION is specified, assuming its value is a full volume name (including catalog
    // and schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(FUNCTION)) {
      String fullName = (String) resourceKeys.get(FUNCTION);
      FunctionInfo function = functionRepository.getFunction(fullName);
      String fullSchemaName = function.getCatalogName() + "." + function.getSchemaName();
      SchemaInfo schema = schemaRepository.getSchema(fullSchemaName);
      CatalogInfo catalog = catalogRepository.getCatalog(function.getCatalogName());
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = modelRepository.getRegisteredModel(fullName);
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
    }

    // If only REGISTERED_MODEL is specified, assuming its value is a full volume name (including
    // catalog and schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName = (String) resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = modelRepository.getRegisteredModel(fullName);
      String fullSchemaName = model.getCatalogName() + "." + model.getSchemaName();
      SchemaInfo schema = schemaRepository.getSchema(fullSchemaName);
      CatalogInfo catalog = catalogRepository.getCatalog(model.getCatalogName());
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA);
      SchemaInfo schema = schemaRepository.getSchema(fullName);
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
    }

    // if only SCHEMA is specified, assuming its value is a full schema name (including catalog)
    if (!resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = (String) resourceKeys.get(SCHEMA);
      SchemaInfo schema = schemaRepository.getSchema(fullName);
      CatalogInfo catalog = catalogRepository.getCatalog(schema.getCatalogName());
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)) {
      String fullName = (String) resourceKeys.get(CATALOG);
      CatalogInfo catalog = catalogRepository.getCatalog(fullName);
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(METASTORE)) {
      resourceIds.put(METASTORE, metastoreRepository.getMetastoreId());
    }

    // External locations can be referenced by name (string) or UUID or path
    if (resourceKeys.containsKey(EXTERNAL_LOCATION)) {
      Object resourceObject = resourceKeys.get(EXTERNAL_LOCATION);
      if (resourceObject instanceof UUID) {
        resourceIds.put(EXTERNAL_LOCATION, resourceObject);
      } else {
        String nameOrPath = (String) resourceObject;
        if (nameOrPath.contains("/")) {
          // If the name contains a / then it's the location path.
          // The location path always start with s3://, abfs://, gs://, file:// or even /local.
          // So it's safe to assume that the resource key must be a path.
          // Otherwise, it's a name and name never has a '/' in it.
          // The actual resource keys are generated by getMapResourceKeysForPath according to the
          // actual ownership of the path.
          resourceIds.putAll(
              externalLocationUtils.getMapResourceIdsForPath(NormalizedURL.from(nameOrPath)));
        } else {
          // Otherwise it's the name
          resourceIds.put(
              EXTERNAL_LOCATION,
              UUID.fromString(externalLocationRepository.getExternalLocation(nameOrPath).getId()));
        }
      }
    }

    // Credentials can be referenced by name (string) or UUID
    if (resourceKeys.containsKey(CREDENTIAL)) {
      Object resourceObject = resourceKeys.get(CREDENTIAL);
      if (resourceObject == null) {
        // Credential is explicitly null (e.g., not being updated), don't add to resourceIds
      } else if (resourceObject instanceof UUID) {
        resourceIds.put(CREDENTIAL, resourceObject);
      } else {
        String name = (String) resourceObject;
        String credentialId = credentialRepository.getCredential(name).getId();
        resourceIds.put(CREDENTIAL, UUID.fromString(credentialId));
      }
    }

    return resourceIds;
  }
}
