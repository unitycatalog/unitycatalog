package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnInfos;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.DeltaCommitMetadataProperties;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.DeltaMetadata;
import io.unitycatalog.server.model.DeltaUniform;
import io.unitycatalog.server.model.DeltaUniformIceberg;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.PropertyRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.hibernate.query.MutationQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Armeria annotated service that implements the Delta REST Catalog API.
 *
 * <p>This service handles all the Delta REST API endpoints defined in delta-rest.yaml. It delegates
 * to existing UC repositories (TableRepository, DeltaCommitRepository) for persistence, converting
 * between the delta-rest API types and the existing UC model types.
 *
 * <p>The service is registered at the path prefix "/api/2.1/unity-catalog/delta" so that endpoint
 * paths like "/v1/config" become "/api/2.1/unity-catalog/delta/v1/config".
 */
@ExceptionHandler(DeltaRestExceptionHandler.class)
public class DeltaRestCatalogService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaRestCatalogService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<String> SUPPORTED_ENDPOINTS =
      List.of(
          "GET /v1/config",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/staging-tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/staging-tables/{table_id}/credentials",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "DELETE /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "HEAD /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /v1/catalogs/{catalog}/tables/rename",
          "GET /v1/temporary-path-credentials",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics");

  private static final double MAX_SUPPORTED_PROTOCOL_VERSION = 1.1d;
  private static final Set<String> REQUIRED_READER_FEATURES =
      Set.of("deletionVectors", "vacuumProtocolCheck");
  private static final Set<String> REQUIRED_WRITER_FEATURES =
      Set.of(
          "catalogManaged",
          "deletionVectors",
          "inCommitTimestamp",
          "v2Checkpoint",
          "vacuumProtocolCheck");
  private static final Set<String> SUGGESTED_READER_FEATURES = Set.of("typeWidening");
  private static final Set<String> SUGGESTED_WRITER_FEATURES =
      Set.of("domainMetadata", "rowTracking", "typeWidening");
  private static final Map<String, String> REQUIRED_PROPERTIES_TEMPLATE =
      Map.of("delta.checkpointPolicy", "v2");

  private final Repositories repositories;
  private final StorageCredentialVendor storageCredentialVendor;
  private final TableRepository tableRepository;
  private final DeltaCommitRepository deltaCommitRepository;

  public DeltaRestCatalogService(
      Repositories repositories, StorageCredentialVendor storageCredentialVendor) {
    this.repositories = repositories;
    this.storageCredentialVendor = storageCredentialVendor;
    this.tableRepository = repositories.getTableRepository();
    this.deltaCommitRepository = repositories.getDeltaCommitRepository();
  }

  // ---- Configuration ----

  @Get("/v1/config")
  @ProducesJson
  @SneakyThrows
  public HttpResponse getConfig(
      @Param("catalog") Optional<String> catalog,
      @Param("protocol-versions") Optional<String> protocolVersions) {
    LOGGER.info(
        "Delta REST getConfig: catalog={}, protocol-versions={}",
        catalog.orElse(null),
        protocolVersions.orElse(null));

    if (catalog.isEmpty() || catalog.get().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Query parameter 'catalog' is required");
    }
    if (protocolVersions.isEmpty() || protocolVersions.get().isBlank()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Query parameter 'protocol-versions' is required");
    }

    double negotiatedVersion = negotiateProtocolVersion(protocolVersions.get());
    ObjectNode response = MAPPER.createObjectNode();
    ArrayNode endpoints = MAPPER.createArrayNode();
    SUPPORTED_ENDPOINTS.forEach(endpoints::add);
    response.set("endpoints", endpoints);
    response.put("protocol-version", negotiatedVersion);

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
  }

  // ---- Staging Tables ----

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/staging-tables")
  @ProducesJson
  @SneakyThrows
  public HttpResponse createStagingTable(
      @Param("catalog") String catalog, @Param("schema") String schema, JsonNode requestBody) {
    LOGGER.info(
        "Delta REST createStagingTable: catalog={}, schema={}, body={}",
        catalog,
        schema,
        requestBody);

    String tableName = requestBody.path("name").asText(null);
    if (tableName == null || tableName.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required");
    }

    // Delegate to the existing staging table repository
    io.unitycatalog.server.model.CreateStagingTable createStagingTable =
        new io.unitycatalog.server.model.CreateStagingTable()
            .catalogName(catalog)
            .schemaName(schema)
            .name(tableName);

    io.unitycatalog.server.model.StagingTableInfo stagingTableInfo =
        repositories.getStagingTableRepository().createStagingTable(createStagingTable);

    Map<String, String> requiredProperties = new LinkedHashMap<>(REQUIRED_PROPERTIES_TEMPLATE);
    requiredProperties.put("io.unitycatalog.tableId", stagingTableInfo.getId().toString());

    ObjectNode response = MAPPER.createObjectNode();
    response.put("table-id", stagingTableInfo.getId().toString());
    response.put("table-type", "MANAGED");
    response.put("location", stagingTableInfo.getStagingLocation());
    response.set(
        "storage-credentials",
        vendCredentialsArray(
            NormalizedURL.from(stagingTableInfo.getStagingLocation()),
            Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE)));
    response.set(
        "required-protocol",
        protocolNode(3, 7, REQUIRED_READER_FEATURES, REQUIRED_WRITER_FEATURES));
    response.set(
        "suggested-protocol",
        protocolNode(3, 7, SUGGESTED_READER_FEATURES, SUGGESTED_WRITER_FEATURES));
    response.set("required-properties", MAPPER.valueToTree(requiredProperties));

    ObjectNode suggestedPropertiesNode = MAPPER.createObjectNode();
    suggestedPropertiesNode.putNull("delta.rowTracking.materializedRowIdColumnName");
    suggestedPropertiesNode.putNull("delta.rowTracking.materializedRowCommitVersionColumnName");
    response.set("suggested-properties", suggestedPropertiesNode);

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
  }

  // ---- Staging Table Credentials ----

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/staging-tables/{table_id}/credentials")
  @ProducesJson
  @SneakyThrows
  public HttpResponse getStagingTableCredentials(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table_id") String tableId) {
    LOGGER.info(
        "Delta REST getStagingTableCredentials: catalog={}, schema={}, tableId={}",
        catalog,
        schema,
        tableId);

    NormalizedURL storageLocation =
        tableRepository.getStorageLocationForTableOrStagingTable(UUID.fromString(tableId));

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            credentialsResponse(
                storageLocation,
                Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE))));
  }

  // ---- Tables ----

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/tables")
  @ProducesJson
  @SneakyThrows
  public HttpResponse createTable(
      @Param("catalog") String catalog, @Param("schema") String schema, JsonNode requestBody) {
    LOGGER.info(
        "Delta REST createTable: catalog={}, schema={}, body={}", catalog, schema, requestBody);

    String name = requestBody.path("name").asText(null);
    String location = requestBody.path("location").asText(null);
    String tableTypeStr = requestBody.path("table-type").asText(null);
    String formatStr = requestBody.path("data-source-format").asText(null);
    String comment = requestBody.path("comment").asText(null);

    if (name == null || name.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required");
    }
    if (location == null || location.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table location is required");
    }
    if (tableTypeStr == null || tableTypeStr.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table type is required");
    }
    if (formatStr == null || formatStr.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Data source format is required");
    }
    if (!requestBody.has("columns") || !requestBody.path("columns").isArray()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Columns are required");
    }
    if (!requestBody.has("properties") || !requestBody.path("properties").isObject()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Properties are required");
    }
    if (!requestBody.has("protocol") || !requestBody.path("protocol").isObject()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Protocol is required");
    }

    List<ColumnInfo> columns =
        convertDeltaColumns(requestBody.path("columns"), requestBody.path("partition-columns"));

    Map<String, String> properties = collectPropertiesObject(requestBody.path("properties"));
    applyProtocolToProperties(properties, requestBody.path("protocol"));
    applyDomainMetadataToProperties(properties, requestBody.path("domain-metadata"));
    if (requestBody.has("last-commit-version")) {
      properties.put(
          "delta.lastUpdateVersion",
          String.valueOf(requestBody.path("last-commit-version").asLong()));
    }
    if (requestBody.has("last-commit-timestamp-ms")) {
      properties.put(
          "delta.lastCommitTimestamp",
          String.valueOf(requestBody.path("last-commit-timestamp-ms").asLong()));
    }

    // Build UC CreateTable
    CreateTable createTable =
        new CreateTable()
            .name(name)
            .catalogName(catalog)
            .schemaName(schema)
            .tableType(TableType.fromValue(tableTypeStr))
            .dataSourceFormat(DataSourceFormat.fromValue(formatStr))
            .columns(columns)
            .storageLocation(location)
            .comment(comment)
            .properties(properties);

    TableInfo tableInfo = tableRepository.createTable(createTable);

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            buildLoadTableResponse(
                tableInfo,
                catalog,
                schema,
                requestBody.has("last-commit-version")
                    ? requestBody.path("last-commit-version").asLong()
                    : null,
                requestBody.has("last-commit-timestamp-ms")
                    ? requestBody.path("last-commit-timestamp-ms").asLong()
                    : null)));
  }

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/tables")
  @ProducesJson
  @SneakyThrows
  public HttpResponse listTables(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("maxResults") Optional<Integer> maxResults,
      @Param("pageToken") Optional<String> pageToken) {
    LOGGER.info("Delta REST listTables: catalog={}, schema={}", catalog, schema);

    ListTablesResponse ucResponse =
        tableRepository.listTables(catalog, schema, maxResults, pageToken, false, false);

    // Convert to delta-rest format
    ObjectNode response = MAPPER.createObjectNode();
    ArrayNode identifiers = MAPPER.createArrayNode();

    if (ucResponse.getTables() != null) {
      for (TableInfo tableInfo : ucResponse.getTables()) {
        ObjectNode id = MAPPER.createObjectNode();
        id.put("schema", tableInfo.getSchemaName());
        id.put("name", tableInfo.getName());
        id.put(
            "data-source-format",
            tableInfo.getDataSourceFormat() != null
                ? tableInfo.getDataSourceFormat().getValue()
                : "DELTA");
        identifiers.add(id);
      }
    }
    response.set("identifiers", identifiers);
    if (ucResponse.getNextPageToken() != null) {
      response.put("next-page-token", ucResponse.getNextPageToken());
    }

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
  }

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @SneakyThrows
  public HttpResponse loadTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("with-credentials") Optional<Boolean> withCredentials) {
    LOGGER.info("Delta REST loadTable: catalog={}, schema={}, table={}", catalog, schema, table);

    String fullName = catalog + "." + schema + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);

    TableCommitState commitState = getTableCommitState(tableInfo, fullName);

    Map<String, Object> response =
        buildLoadTableResponse(
            tableInfo,
            catalog,
            schema,
            commitState.latestTableVersion,
            commitState.latestTimestamp);
    // Override commits and latest-table-version with actual data
    response.put("commits", commitState.commitsList);
    if (commitState.latestTableVersion != null) {
      response.put("latest-table-version", commitState.latestTableVersion);
    }

    // Add credentials if requested
    if (withCredentials.orElse(false) && tableInfo.getStorageLocation() != null) {
      NormalizedURL storageLocation = NormalizedURL.from(tableInfo.getStorageLocation());
      response.put(
          "storage-credentials",
          credentialsResponse(
                  storageLocation,
                  Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE))
              .get("storage-credentials"));
    }

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
  }

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  @ProducesJson
  @SneakyThrows
  public HttpResponse updateTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table,
      JsonNode requestBody) {
    LOGGER.info(
        "Delta REST updateTable: catalog={}, schema={}, table={}, body={}",
        catalog,
        schema,
        table,
        requestBody);

    String fullName = catalog + "." + schema + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);
    TableUpdateAccumulator accumulator = new TableUpdateAccumulator(tableInfo);

    // Process requirements
    JsonNode requirements = requestBody.path("requirements");
    if (requirements.isArray()) {
      for (JsonNode req : requirements) {
        String type = req.path("type").asText();
        switch (type) {
          case "assert-table-uuid":
            String expectedUuid = req.path("uuid").asText();
            if (!expectedUuid.equals(tableInfo.getTableId())) {
              throw new BaseException(
                  ErrorCode.FAILED_PRECONDITION,
                  String.format(
                      "Table UUID mismatch: expected %s, got %s",
                      expectedUuid, tableInfo.getTableId()));
            }
            break;
          case "assert-etag":
            String expectedEtag = req.path("etag").asText();
            TableCommitState currentCommitState = getTableCommitState(tableInfo, fullName);
            @SuppressWarnings("unchecked")
            Map<String, Object> currentMetadata =
                (Map<String, Object>)
                    buildLoadTableResponse(
                            tableInfo,
                            catalog,
                            schema,
                            currentCommitState.latestTableVersion,
                            currentCommitState.latestTimestamp)
                        .get("metadata");
            String currentEtag = (String) currentMetadata.get("etag");
            if (!expectedEtag.equals(currentEtag)) {
              throw new BaseException(
                  ErrorCode.ABORTED,
                  String.format("Etag mismatch: expected %s, got %s", expectedEtag, currentEtag));
            }
            break;
          default:
            LOGGER.warn("Unknown requirement type: {}", type);
        }
      }
    }

    Long latestBackfilledVersion = null;
    DeltaCommitInfo commitInfo = null;
    DeltaUniform uniform = null;
    JsonNode updates = requestBody.path("updates");
    if (updates.isArray()) {
      for (JsonNode update : updates) {
        String action = update.path("action").asText();
        switch (action) {
          case "set-properties":
            accumulator.properties.putAll(collectPropertiesObject(update.path("updates")));
            break;
          case "remove-properties":
            update.path("removals").forEach(v -> accumulator.properties.remove(v.asText()));
            break;
          case "set-protocol":
            applyProtocolToProperties(accumulator.properties, update.path("protocol"));
            break;
          case "set-columns":
            accumulator.columns = convertDeltaColumns(update.path("columns"), null);
            break;
          case "set-partition-columns":
            accumulator.columns =
                applyPartitionColumns(
                    accumulator.columns != null
                        ? accumulator.columns
                        : accumulator.currentColumns(),
                    update.path("partition-columns"));
            break;
          case "set-table-comment":
            accumulator.comment = update.path("comment").asText(null);
            break;
          case "set-domain-metadata":
            applyDomainMetadataToProperties(
                accumulator.properties,
                update.path("domain").asText(),
                update.path("configuration"));
            break;
          case "remove-domain-metadata":
            removeDomainMetadataFromProperties(accumulator.properties, update.path("domains"));
            break;
          case "add-commit":
            commitInfo = toDeltaCommitInfo(update.path("commit"));
            accumulator.properties.put(
                "delta.lastUpdateVersion", String.valueOf(commitInfo.getVersion()));
            accumulator.properties.put(
                "delta.lastCommitTimestamp", String.valueOf(commitInfo.getTimestamp()));
            uniform = toUniform(update.path("uniform"));
            break;
          case "set-latest-backfilled-version":
            latestBackfilledVersion = update.path("latest-published-version").asLong();
            break;
          case "update-snapshot-version":
            accumulator.properties.put(
                "delta.lastUpdateVersion",
                String.valueOf(update.path("last-commit-version").asLong()));
            accumulator.properties.put(
                "delta.lastCommitTimestamp",
                String.valueOf(update.path("last-commit-timestamp-ms").asLong()));
            break;
          default:
            LOGGER.warn("Unknown update action: {}", action);
        }
      }
    }

    if (commitInfo != null) {
      DeltaCommit deltaCommit =
          new DeltaCommit()
              .tableId(tableInfo.getTableId())
              .tableUri(tableInfo.getStorageLocation())
              .commitInfo(commitInfo)
              .latestBackfilledVersion(latestBackfilledVersion);
      if (accumulator.hasMetadataChanges()) {
        deltaCommit.metadata(accumulator.toDeltaMetadata());
      }
      if (uniform != null) {
        deltaCommit.uniform(uniform);
      }
      deltaCommitRepository.postCommit(deltaCommit);
    } else {
      if (accumulator.hasMetadataChanges()) {
        applyMetadataOnlyUpdate(tableInfo, accumulator);
      }
      if (latestBackfilledVersion != null) {
        deltaCommitRepository.postCommit(
            new DeltaCommit()
                .tableId(tableInfo.getTableId())
                .tableUri(tableInfo.getStorageLocation())
                .latestBackfilledVersion(latestBackfilledVersion));
      }
    }

    Long responseLastCommitVersion =
        commitInfo != null
            ? commitInfo.getVersion()
            : parseLongOrNull(accumulator.properties.get("delta.lastUpdateVersion"));
    Long responseLastCommitTimestamp =
        commitInfo != null
            ? commitInfo.getTimestamp()
            : parseLongOrNull(accumulator.properties.get("delta.lastCommitTimestamp"));

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            buildLoadTableResponse(
                tableRepository.getTable(fullName),
                catalog,
                schema,
                responseLastCommitVersion,
                responseLastCommitTimestamp)));
  }

  @Delete("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public HttpResponse deleteTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table) {
    LOGGER.info("Delta REST deleteTable: catalog={}, schema={}, table={}", catalog, schema, table);
    String fullName = catalog + "." + schema + "." + table;
    tableRepository.deleteTable(fullName);
    return noContentResponse();
  }

  @Head("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public HttpResponse tableExists(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table) {
    String fullName = catalog + "." + schema + "." + table;
    try {
      tableRepository.getTable(fullName);
      return noContentResponse();
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        return HttpResponse.of(
            ResponseHeaders.builder(HttpStatus.NOT_FOUND)
                .addInt(HttpHeaderNames.CONTENT_LENGTH, 0)
                .build());
      }
      throw e;
    }
  }

  // ---- Table Credentials ----

  @Get("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials")
  @ProducesJson
  @SneakyThrows
  public HttpResponse getTableCredentials(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table) {
    LOGGER.info(
        "Delta REST getTableCredentials: catalog={}, schema={}, table={}", catalog, schema, table);

    NormalizedURL storageLocation = resolveTableOrStagingLocation(catalog, schema, table);
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            credentialsResponse(
                storageLocation,
                Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE))));
  }

  // ---- Rename Table ----

  @Post("/v1/catalogs/{catalog}/tables/rename")
  public HttpResponse renameTable(@Param("catalog") String catalog, JsonNode requestBody) {
    LOGGER.info("Delta REST renameTable: catalog={}, body={}", catalog, requestBody);
    JsonNode source = requestBody.path("source");
    JsonNode destination = requestBody.path("destination");
    String sourceCatalog = source.path("catalog").asText(null);
    String sourceSchema = source.path("schema").asText(null);
    String sourceName = source.path("name").asText(null);
    String destinationName = destination.path("name").asText(null);
    if (sourceCatalog == null
        || sourceSchema == null
        || sourceName == null
        || destinationName == null
        || sourceCatalog.isBlank()
        || sourceSchema.isBlank()
        || sourceName.isBlank()
        || destinationName.isBlank()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Rename source and destination are required");
    }
    if (!catalog.equals(sourceCatalog)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Rename source catalog must match the request path catalog");
    }
    if (destination.has("catalog") || destination.has("schema")) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Rename can only change the table name within the same schema");
    }
    renameTableInPlace(sourceCatalog, sourceSchema, sourceName, destinationName);
    return noContentResponse();
  }

  // ---- Temporary Path Credentials ----

  @Get("/v1/temporary-path-credentials")
  @ProducesJson
  @SneakyThrows
  public HttpResponse getTemporaryPathCredentials(
      @Param("location") String location, @Param("operation") String operation) {
    LOGGER.info(
        "Delta REST getTemporaryPathCredentials: location={}, operation={}", location, operation);

    NormalizedURL storageLocation = NormalizedURL.from(location);
    Set<CredentialContext.Privilege> privileges;
    switch (operation) {
      case "PATH_READ":
        privileges = Set.of(CredentialContext.Privilege.SELECT);
        break;
      case "PATH_READ_WRITE":
      case "PATH_CREATE_TABLE":
        privileges = Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);
        break;
      default:
        privileges = Set.of(CredentialContext.Privilege.SELECT);
        break;
    }

    try {
      var credentials = storageCredentialVendor.vendCredential(storageLocation, privileges);
      return HttpResponse.of(
          HttpStatus.OK,
          MediaType.JSON,
          MAPPER.writeValueAsString(credentialsResponse(storageLocation, privileges)));
    } catch (Exception e) {
      LOGGER.warn("Could not vend credentials for path {}: {}", location, e.getMessage());
      ObjectNode response = MAPPER.createObjectNode();
      response.set("storage-credentials", MAPPER.createArrayNode());
      return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
    }
  }

  // ---- Metrics ----

  @Post("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics")
  public HttpResponse reportMetrics(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table,
      JsonNode requestBody) {
    LOGGER.info(
        "Delta REST reportMetrics: catalog={}, schema={}, table={}", catalog, schema, table);
    // For POC, just acknowledge receipt
    return noContentResponse();
  }

  // ---- Helper Methods ----

  private double negotiateProtocolVersion(String protocolVersions) {
    double best = -1d;
    for (String candidate : protocolVersions.split(",")) {
      String trimmed = candidate.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        double parsed = Double.parseDouble(trimmed);
        int major = (int) parsed;
        if (major == 1) {
          best = Math.max(best, Math.min(parsed, MAX_SUPPORTED_PROTOCOL_VERSION));
        }
      } catch (NumberFormatException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid protocol version '" + trimmed + "' in 'protocol-versions'");
      }
    }
    if (best < 0) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "No mutually supported protocol version found in 'protocol-versions'");
    }
    return best;
  }

  private ObjectNode protocolNode(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    ObjectNode protocol = MAPPER.createObjectNode();
    protocol.put("min-reader-version", minReaderVersion);
    protocol.put("min-writer-version", minWriterVersion);
    ArrayNode readerArray = MAPPER.createArrayNode();
    readerFeatures.forEach(readerArray::add);
    protocol.set("reader-features", readerArray);
    ArrayNode writerArray = MAPPER.createArrayNode();
    writerFeatures.forEach(writerArray::add);
    protocol.set("writer-features", writerArray);
    return protocol;
  }

  private Map<String, String> collectPropertiesObject(JsonNode node) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (node != null && node.isObject()) {
      node.fields()
          .forEachRemaining(e -> properties.put(e.getKey(), jsonValueToString(e.getValue())));
    }
    return properties;
  }

  private String jsonValueToString(JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    if (value.isTextual()) {
      return value.asText();
    }
    if (value.isNumber() || value.isBoolean()) {
      return value.asText();
    }
    return value.toString();
  }

  private void applyProtocolToProperties(Map<String, String> properties, JsonNode protocolNode) {
    if (protocolNode == null || !protocolNode.isObject()) {
      return;
    }
    if (protocolNode.has("min-reader-version")) {
      properties.put(
          "delta.minReaderVersion",
          String.valueOf(protocolNode.path("min-reader-version").asInt()));
    }
    if (protocolNode.has("min-writer-version")) {
      properties.put(
          "delta.minWriterVersion",
          String.valueOf(protocolNode.path("min-writer-version").asInt()));
    }
    Set<String> desiredFeatures = new LinkedHashSet<>();
    protocolNode.path("reader-features").forEach(v -> desiredFeatures.add(v.asText()));
    protocolNode.path("writer-features").forEach(v -> desiredFeatures.add(v.asText()));
    properties.entrySet().removeIf(e -> e.getKey().startsWith("delta.feature."));
    for (String feature : desiredFeatures) {
      properties.put("delta.feature." + feature, "supported");
      maybeApplyFeatureActivationProperty(properties, feature, true);
    }
  }

  private void maybeApplyFeatureActivationProperty(
      Map<String, String> properties, String feature, boolean enabled) {
    String enabledValue = enabled ? "true" : "false";
    switch (feature) {
      case "deletionVectors":
        properties.put("delta.enableDeletionVectors", enabledValue);
        break;
      case "rowTracking":
        properties.put("delta.enableRowTracking", enabledValue);
        break;
      case "inCommitTimestamp":
        properties.put("delta.enableInCommitTimestamps", enabledValue);
        break;
      case "v2Checkpoint":
        properties.put("delta.checkpointPolicy", enabled ? "v2" : "classic");
        break;
      default:
        break;
    }
  }

  private void applyDomainMetadataToProperties(
      Map<String, String> properties, JsonNode domainsNode) {
    if (domainsNode == null || !domainsNode.isObject()) {
      return;
    }
    domainsNode
        .fields()
        .forEachRemaining(
            e -> applyDomainMetadataToProperties(properties, e.getKey(), e.getValue()));
  }

  private void applyDomainMetadataToProperties(
      Map<String, String> properties, String domain, JsonNode configuration) {
    if (domain == null || configuration == null || configuration.isMissingNode()) {
      return;
    }
    switch (domain) {
      case "delta.clustering":
        JsonNode clusteringColumns = configuration.path("clusteringColumns");
        if (!clusteringColumns.isMissingNode()) {
          properties.put("clusteringColumns", clusteringColumns.toString());
        }
        break;
      case "delta.rowTracking":
        properties.put("delta.rowTracking", configuration.toString());
        if (configuration.has("rowIdHighWaterMark")) {
          properties.put(
              "delta.rowTracking.rowIdHighWaterMark",
              configuration.path("rowIdHighWaterMark").asText());
        }
        maybeApplyFeatureActivationProperty(properties, "rowTracking", true);
        break;
      default:
        properties.put("delta.domainMetadata." + domain, configuration.toString());
        break;
    }
  }

  private void removeDomainMetadataFromProperties(
      Map<String, String> properties, JsonNode domainsNode) {
    if (domainsNode == null || !domainsNode.isArray()) {
      return;
    }
    domainsNode.forEach(domain -> removeDomainMetadataFromProperties(properties, domain.asText()));
  }

  private void removeDomainMetadataFromProperties(Map<String, String> properties, String domain) {
    switch (domain) {
      case "delta.clustering":
        properties.remove("clusteringColumns");
        break;
      case "delta.rowTracking":
        properties.remove("delta.rowTracking");
        properties.remove("delta.rowTracking.rowIdHighWaterMark");
        break;
      default:
        properties.remove("delta.domainMetadata." + domain);
        break;
    }
  }

  private List<ColumnInfo> convertDeltaColumns(JsonNode columnsNode, JsonNode partitionColumnsNode)
      throws Exception {
    List<ColumnInfo> columns = new ArrayList<>();
    if (columnsNode == null || !columnsNode.isArray()) {
      return columns;
    }
    Map<String, Integer> partitionPositions = new LinkedHashMap<>();
    if (partitionColumnsNode != null && partitionColumnsNode.isArray()) {
      int idx = 0;
      for (JsonNode partitionColumn : partitionColumnsNode) {
        partitionPositions.put(partitionColumn.asText(), idx++);
      }
    }
    int position = 0;
    for (JsonNode col : columnsNode) {
      ColumnInfo columnInfo = new ColumnInfo();
      columnInfo.setName(col.path("name").asText());
      JsonNode typeNode = col.path("type");
      String typeText = typeNode.isTextual() ? typeNode.asText() : typeNode.toString();
      columnInfo.setTypeText(typeText);
      columnInfo.setTypeJson(MAPPER.writeValueAsString(col));
      columnInfo.setTypeName(
          io.unitycatalog.server.model.ColumnTypeName.fromValue(mapDeltaTypeToUCType(typeText)));
      columnInfo.setNullable(col.path("nullable").asBoolean(true));
      columnInfo.setPosition(position++);
      if (partitionPositions.containsKey(columnInfo.getName())) {
        columnInfo.setPartitionIndex(partitionPositions.get(columnInfo.getName()));
      }
      JsonNode metadata = col.path("metadata");
      if (metadata.has("comment")) {
        columnInfo.setComment(metadata.path("comment").asText());
      }
      columns.add(columnInfo);
    }
    return columns;
  }

  private List<ColumnInfo> applyPartitionColumns(
      List<ColumnInfo> columns, JsonNode partitionColumnsNode) {
    Map<String, Integer> partitionPositions = new LinkedHashMap<>();
    if (partitionColumnsNode != null && partitionColumnsNode.isArray()) {
      int idx = 0;
      for (JsonNode partitionColumn : partitionColumnsNode) {
        partitionPositions.put(partitionColumn.asText(), idx++);
      }
    }
    for (ColumnInfo column : columns) {
      column.setPartitionIndex(partitionPositions.get(column.getName()));
    }
    return columns;
  }

  private DeltaCommitInfo toDeltaCommitInfo(JsonNode commitNode) {
    return new DeltaCommitInfo()
        .version(commitNode.path("version").asLong())
        .timestamp(commitNode.path("timestamp").asLong())
        .fileName(commitNode.path("file-name").asText())
        .fileSize(commitNode.path("file-size").asLong())
        .fileModificationTimestamp(commitNode.path("file-modification-timestamp").asLong());
  }

  private DeltaUniform toUniform(JsonNode uniformNode) {
    if (uniformNode == null || uniformNode.isMissingNode() || !uniformNode.has("iceberg")) {
      return null;
    }
    JsonNode icebergNode = uniformNode.path("iceberg");
    String metadataLocation = icebergNode.path("metadata-location").asText(null);
    return new DeltaUniform()
        .iceberg(
            new DeltaUniformIceberg()
                .metadataLocation(
                    metadataLocation != null ? java.net.URI.create(metadataLocation) : null)
                .convertedDeltaVersion(
                    icebergNode.has("converted-delta-version")
                        ? icebergNode.path("converted-delta-version").asLong()
                        : null)
                .convertedDeltaTimestamp(
                    icebergNode.has("converted-delta-timestamp")
                        ? icebergNode.path("converted-delta-timestamp").asText(null)
                        : null));
  }

  private void applyMetadataOnlyUpdate(TableInfo tableInfo, TableUpdateAccumulator accumulator) {
    TransactionManager.executeWithTransaction(
        repositories.getSessionFactory(),
        session -> {
          TableInfoDAO tableInfoDAO =
              session.get(TableInfoDAO.class, UUID.fromString(tableInfo.getTableId()));
          if (tableInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Table not found: " + tableInfo.getTableId());
          }
          PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::remove);
          session.flush();
          PropertyDAO.from(accumulator.properties, tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::persist);
          List<ColumnInfoDAO> newColumns = ColumnInfoDAO.fromList(accumulator.columns);
          tableInfoDAO.getColumns().clear();
          session.flush();
          newColumns.forEach(
              c -> {
                c.setId(UUID.randomUUID());
                c.setTable(tableInfoDAO);
              });
          tableInfoDAO.getColumns().addAll(newColumns);
          tableInfoDAO.setComment(accumulator.comment);
          tableInfoDAO.setUpdatedBy(IdentityUtils.findPrincipalEmailAddress());
          tableInfoDAO.setUpdatedAt(new Date());
          session.merge(tableInfoDAO);
          return null;
        },
        "Failed to update Delta REST table metadata",
        false);
  }

  private NormalizedURL resolveTableOrStagingLocation(
      String catalog, String schema, String tableRef) {
    try {
      return tableRepository.getStorageLocationForTableOrStagingTable(UUID.fromString(tableRef));
    } catch (IllegalArgumentException | BaseException ignored) {
      TableInfo tableInfo = tableRepository.getTable(catalog + "." + schema + "." + tableRef);
      if (tableInfo.getStorageLocation() == null) {
        throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table has no storage location");
      }
      return NormalizedURL.from(tableInfo.getStorageLocation());
    }
  }

  private Map<String, Object> credentialsResponse(
      NormalizedURL storageLocation, Set<CredentialContext.Privilege> privileges) {
    try {
      var credentials = storageCredentialVendor.vendCredential(storageLocation, privileges);
      return convertCredentialsToResponse(storageLocation, credentials);
    } catch (Exception e) {
      LOGGER.warn("Could not vend credentials for {}: {}", storageLocation, e.getMessage());
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("storage-credentials", Collections.emptyList());
      return response;
    }
  }

  private ArrayNode vendCredentialsArray(
      NormalizedURL storageLocation, Set<CredentialContext.Privilege> privileges) {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> credentials =
        (List<Map<String, Object>>)
            credentialsResponse(storageLocation, privileges).get("storage-credentials");
    return MAPPER.valueToTree(credentials);
  }

  private HttpResponse noContentResponse() {
    return HttpResponse.of(
        ResponseHeaders.builder(HttpStatus.NO_CONTENT)
            .addInt(HttpHeaderNames.CONTENT_LENGTH, 0)
            .build(),
        HttpData.empty());
  }

  private TableInfo getTableWithRetry(String fullName) {
    BaseException lastException = null;
    for (int attempt = 0; attempt < 20; attempt++) {
      try {
        return tableRepository.getTable(fullName);
      } catch (BaseException e) {
        if (e.getErrorCode() != ErrorCode.NOT_FOUND) {
          throw e;
        }
        lastException = e;
      }
      try {
        Thread.sleep(100L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BaseException(
            ErrorCode.INTERNAL, "Interrupted while waiting for table visibility: " + fullName);
      }
    }
    throw lastException;
  }

  private void renameTableInPlace(
      String catalog, String schema, String sourceName, String destinationName) {
    TableInfo sourceTable = getTableWithRetry(catalog + "." + schema + "." + sourceName);
    TransactionManager.executeWithTransaction(
        repositories.getSessionFactory(),
        session -> {
          UUID schemaId =
              repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalog, schema);
          TableInfoDAO destination =
              tableRepository.findBySchemaIdAndName(session, schemaId, destinationName);
          if (destination != null) {
            throw new BaseException(
                ErrorCode.TABLE_ALREADY_EXISTS,
                "Table already exists: " + catalog + "." + schema + "." + destinationName);
          }
          MutationQuery query =
              session.createMutationQuery(
                  "UPDATE TableInfoDAO t "
                      + "SET t.name = :destinationName, "
                      + "t.updatedBy = :updatedBy, "
                      + "t.updatedAt = :updatedAt "
                      + "WHERE t.id = :tableId");
          query.setParameter("destinationName", destinationName);
          query.setParameter("updatedBy", IdentityUtils.findPrincipalEmailAddress());
          query.setParameter("updatedAt", new Date());
          query.setParameter("tableId", UUID.fromString(sourceTable.getTableId()));
          if (query.executeUpdate() != 1) {
            throw new BaseException(
                ErrorCode.NOT_FOUND,
                "Table not found: " + catalog + "." + schema + "." + sourceName);
          }
          return null;
        },
        "Failed to rename Delta REST table",
        false);
  }

  private Long parseLongOrNull(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private TableCommitState getTableCommitState(TableInfo tableInfo, String fullName) {
    Long latestTableVersion = null;
    Long latestTimestamp = null;
    List<Map<String, Object>> commitsList = new ArrayList<>();
    if (tableInfo.getTableType() == TableType.MANAGED && tableInfo.getTableId() != null) {
      try {
        DeltaGetCommits getCommitsReq = new DeltaGetCommits();
        getCommitsReq.setTableId(tableInfo.getTableId());
        getCommitsReq.setStartVersion(0L);
        DeltaGetCommitsResponse commitsResp = deltaCommitRepository.getCommits(getCommitsReq);
        if (commitsResp != null) {
          latestTableVersion = commitsResp.getLatestTableVersion();
          if (commitsResp.getCommits() != null) {
            for (var c : commitsResp.getCommits()) {
              Map<String, Object> cm = new LinkedHashMap<>();
              cm.put("version", c.getVersion());
              cm.put("timestamp", c.getTimestamp());
              cm.put("file-name", c.getFileName());
              cm.put("file-size", c.getFileSize());
              cm.put("file-modification-timestamp", c.getFileModificationTimestamp());
              commitsList.add(cm);
            }
            if (!commitsResp.getCommits().isEmpty()) {
              latestTimestamp = commitsResp.getCommits().get(0).getTimestamp();
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Could not fetch commits for table {}: {}", fullName, e.getMessage());
      }
    }
    return new TableCommitState(latestTableVersion, latestTimestamp, commitsList);
  }

  private final class TableUpdateAccumulator {
    private final TableInfo current;
    private final Map<String, String> properties;
    private List<ColumnInfo> columns;
    private String comment;

    private TableUpdateAccumulator(TableInfo current) {
      this.current = current;
      this.properties =
          current.getProperties() != null
              ? new LinkedHashMap<>(current.getProperties())
              : new LinkedHashMap<>();
      this.columns =
          current.getColumns() != null ? new ArrayList<>(current.getColumns()) : new ArrayList<>();
      this.comment = current.getComment();
    }

    private List<ColumnInfo> currentColumns() {
      return columns != null ? columns : List.of();
    }

    private boolean hasMetadataChanges() {
      return !properties.equals(
              current.getProperties() != null ? current.getProperties() : Collections.emptyMap())
          || !Objects.equals(comment, current.getComment())
          || !sameColumns(columns, current.getColumns());
    }

    private DeltaMetadata toDeltaMetadata() {
      return new DeltaMetadata()
          .description(comment)
          .properties(new DeltaCommitMetadataProperties().properties(properties))
          .schema(new ColumnInfos().columns(columns));
    }
  }

  private static final class TableCommitState {
    private final Long latestTableVersion;
    private final Long latestTimestamp;
    private final List<Map<String, Object>> commitsList;

    private TableCommitState(
        Long latestTableVersion, Long latestTimestamp, List<Map<String, Object>> commitsList) {
      this.latestTableVersion = latestTableVersion;
      this.latestTimestamp = latestTimestamp;
      this.commitsList = commitsList;
    }
  }

  /** Builds a LoadTableResponse map from a UC TableInfo. */
  private Map<String, Object> buildLoadTableResponse(
      TableInfo tableInfo,
      String catalog,
      String schema,
      Long lastCommitVersion,
      Long lastCommitTimestamp) {
    Map<String, Object> response = new LinkedHashMap<>();
    Map<String, String> props =
        tableInfo.getProperties() != null ? tableInfo.getProperties() : Map.of();

    // ---- Build metadata ----
    Map<String, Object> metadata = new LinkedHashMap<>();

    metadata.put(
        "etag",
        tableInfo.getTableId()
            + ":"
            + (tableInfo.getUpdatedAt() != null ? tableInfo.getUpdatedAt() : 0)
            + ":"
            + (lastCommitVersion != null ? lastCommitVersion : 0));
    metadata.put(
        "data-source-format",
        tableInfo.getDataSourceFormat() != null
            ? tableInfo.getDataSourceFormat().getValue()
            : "DELTA");
    metadata.put(
        "table-type",
        tableInfo.getTableType() != null ? tableInfo.getTableType().getValue() : "EXTERNAL");
    metadata.put("table-uuid", tableInfo.getTableId());
    metadata.put("location", tableInfo.getStorageLocation());
    metadata.put("owner", tableInfo.getOwner());
    metadata.put("comment", tableInfo.getComment());
    metadata.put("created-time", tableInfo.getCreatedAt());
    metadata.put("created-by", tableInfo.getCreatedBy());
    metadata.put("updated-time", tableInfo.getUpdatedAt());
    metadata.put("updated-by", tableInfo.getUpdatedBy());
    metadata.put("securable-type", "TABLE");

    // ---- Columns: convert to delta-rest type-JSON format ----
    List<Map<String, Object>> columns = new ArrayList<>();
    List<String> partitionColumns = new ArrayList<>();
    if (tableInfo.getColumns() != null) {
      for (ColumnInfo col : tableInfo.getColumns()) {
        Map<String, Object> deltaCol = new LinkedHashMap<>();
        deltaCol.put("name", col.getName());
        if (col.getTypeJson() != null) {
          try {
            JsonNode typeJson = MAPPER.readTree(col.getTypeJson());
            if (typeJson.has("type")) {
              JsonNode typeVal = typeJson.path("type");
              if (typeVal.isTextual()) {
                deltaCol.put("type", typeVal.asText());
              } else {
                deltaCol.put("type", MAPPER.readValue(typeVal.toString(), Object.class));
              }
            } else {
              deltaCol.put(
                  "type", col.getTypeText() != null ? col.getTypeText().toLowerCase() : "string");
            }
            deltaCol.put("nullable", typeJson.path("nullable").asBoolean(true));
            deltaCol.put(
                "metadata",
                typeJson.has("metadata")
                    ? MAPPER.readValue(typeJson.path("metadata").toString(), Object.class)
                    : Map.of());
          } catch (Exception e) {
            deltaCol.put(
                "type", col.getTypeText() != null ? col.getTypeText().toLowerCase() : "string");
            deltaCol.put("nullable", col.getNullable() != null ? col.getNullable() : true);
            deltaCol.put("metadata", Map.of());
          }
        } else {
          deltaCol.put(
              "type", col.getTypeText() != null ? col.getTypeText().toLowerCase() : "string");
          deltaCol.put("nullable", col.getNullable() != null ? col.getNullable() : true);
          deltaCol.put("metadata", Map.of());
        }
        columns.add(deltaCol);
        // Track partition columns
        if (col.getPartitionIndex() != null) {
          while (partitionColumns.size() <= col.getPartitionIndex()) {
            partitionColumns.add(null);
          }
          partitionColumns.set(col.getPartitionIndex(), col.getName());
        }
      }
    }
    // Remove nulls from partition columns list
    partitionColumns.removeIf(Objects::isNull);
    metadata.put("columns", columns);
    metadata.put("partition-columns", partitionColumns);

    // ---- Protocol: derive from properties ----
    Map<String, Object> protocol = new LinkedHashMap<>();
    int minReaderVersion = 1;
    int minWriterVersion = 2;
    List<String> readerFeatures = new ArrayList<>();
    List<String> writerFeatures = new ArrayList<>();

    if (props.containsKey("delta.minReaderVersion")) {
      try {
        minReaderVersion = Integer.parseInt(props.get("delta.minReaderVersion"));
      } catch (NumberFormatException e) {
        LOGGER.debug("Failed to parse number from property", e);
      }
    }
    if (props.containsKey("delta.minWriterVersion")) {
      try {
        minWriterVersion = Integer.parseInt(props.get("delta.minWriterVersion"));
      } catch (NumberFormatException e) {
        LOGGER.debug("Failed to parse number from property", e);
      }
    }
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (entry.getKey().startsWith("delta.feature.") && "supported".equals(entry.getValue())) {
        String feature = entry.getKey().substring("delta.feature.".length());
        writerFeatures.add(feature);
        // Reader-writer features appear in both lists
        if (isReaderFeature(feature)) {
          readerFeatures.add(feature);
        }
      }
    }
    protocol.put("min-reader-version", minReaderVersion);
    protocol.put("min-writer-version", minWriterVersion);
    if (!readerFeatures.isEmpty()) {
      protocol.put("reader-features", readerFeatures);
    }
    if (!writerFeatures.isEmpty()) {
      protocol.put("writer-features", writerFeatures);
    }
    metadata.put("protocol", protocol);

    // ---- Properties: keep all including derived (per spec, GET repeats them) ----
    metadata.put("properties", props);

    // ---- Snapshot-derived first-class fields ----
    Long lcv = lastCommitVersion;
    Long lct = lastCommitTimestamp;
    if (lcv == null && props.containsKey("delta.lastUpdateVersion")) {
      try {
        lcv = Long.parseLong(props.get("delta.lastUpdateVersion"));
      } catch (NumberFormatException e) {
        LOGGER.debug("Failed to parse number from property", e);
      }
    }
    if (lct == null && props.containsKey("delta.lastCommitTimestamp")) {
      try {
        lct = Long.parseLong(props.get("delta.lastCommitTimestamp"));
      } catch (NumberFormatException e) {
        LOGGER.debug("Failed to parse number from property", e);
      }
    }
    if (lcv != null) {
      metadata.put("last-commit-version", lcv);
    }
    if (lct != null) {
      metadata.put("last-commit-timestamp-ms", lct);
    }

    response.put("metadata", metadata);

    // ---- Top-level fields ----
    response.put("latest-table-version", lcv != null ? lcv : 0L);
    response.put("commits", List.of());

    return response;
  }

  private boolean sameColumns(List<ColumnInfo> left, List<ColumnInfo> right) {
    List<ColumnInfo> lhs = left != null ? left : List.of();
    List<ColumnInfo> rhs = right != null ? right : List.of();
    if (lhs.size() != rhs.size()) {
      return false;
    }
    for (int i = 0; i < lhs.size(); i++) {
      ColumnInfo a = lhs.get(i);
      ColumnInfo b = rhs.get(i);
      if (!Objects.equals(a.getName(), b.getName())
          || !Objects.equals(a.getTypeText(), b.getTypeText())
          || !Objects.equals(a.getTypeJson(), b.getTypeJson())
          || !Objects.equals(a.getNullable(), b.getNullable())
          || !Objects.equals(a.getPartitionIndex(), b.getPartitionIndex())
          || !Objects.equals(a.getComment(), b.getComment())) {
        return false;
      }
    }
    return true;
  }

  /** Known reader-writer features (appear in both reader-features and writer-features). */
  private static final Set<String> READER_FEATURES =
      Set.of(
          "deletionVectors",
          "columnMapping",
          "timestampNtz",
          "typeWidening",
          "v2Checkpoint",
          "vacuumProtocolCheck",
          "catalogManaged");

  private boolean isReaderFeature(String feature) {
    return READER_FEATURES.contains(feature);
  }

  /** Converts UC TemporaryCredentials to the delta-rest CredentialsResponse format. */
  private Map<String, Object> convertCredentialsToResponse(
      NormalizedURL storageLocation,
      io.unitycatalog.server.model.TemporaryCredentials credentials) {
    Map<String, Object> response = new LinkedHashMap<>();
    List<Map<String, Object>> storageCredentials = new ArrayList<>();

    Map<String, Object> cred = new LinkedHashMap<>();
    cred.put("prefix", storageLocation.toString());

    Map<String, String> config = new HashMap<>();
    // Extract credentials based on the type
    if (credentials.getAwsTempCredentials() != null) {
      var aws = credentials.getAwsTempCredentials();
      if (aws.getAccessKeyId() != null) {
        config.put("s3.access-key-id", aws.getAccessKeyId());
      }
      if (aws.getSecretAccessKey() != null) {
        config.put("s3.secret-access-key", aws.getSecretAccessKey());
      }
      if (aws.getSessionToken() != null) {
        config.put("s3.session-token", aws.getSessionToken());
      }
    }
    if (credentials.getAzureUserDelegationSas() != null) {
      var azure = credentials.getAzureUserDelegationSas();
      if (azure.getSasToken() != null) config.put("azure.sas-token", azure.getSasToken());
    }
    if (credentials.getGcpOauthToken() != null) {
      var gcp = credentials.getGcpOauthToken();
      if (gcp.getOauthToken() != null) config.put("gcs.oauth-token", gcp.getOauthToken());
    }

    cred.put("config", config);
    if (credentials.getExpirationTime() != null) {
      cred.put("expiration-time-ms", credentials.getExpirationTime());
    }
    storageCredentials.add(cred);

    response.put("storage-credentials", storageCredentials);
    return response;
  }

  /**
   * Maps a Delta type string to a UC ColumnTypeName value. Handles primitive types; complex types
   * default to STRING.
   */
  private String mapDeltaTypeToUCType(String deltaType) {
    if (deltaType == null) return "STRING";
    switch (deltaType.toLowerCase()) {
      case "long":
        return "LONG";
      case "int":
      case "integer":
        return "INT";
      case "short":
        return "SHORT";
      case "byte":
        return "BYTE";
      case "float":
        return "FLOAT";
      case "double":
        return "DOUBLE";
      case "string":
        return "STRING";
      case "boolean":
        return "BOOLEAN";
      case "binary":
        return "BINARY";
      case "date":
        return "DATE";
      case "timestamp":
        return "TIMESTAMP";
      case "timestamp_ntz":
        return "TIMESTAMP_NTZ";
      default:
        // Complex types (struct, array, map, decimal) or unknown
        if (deltaType.startsWith("decimal")) {
          return "DECIMAL";
        }
        return "STRING";
    }
  }
}
