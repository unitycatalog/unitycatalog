package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
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
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "DELETE /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "HEAD /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials");

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

    ObjectNode response = MAPPER.createObjectNode();
    ArrayNode endpoints = MAPPER.createArrayNode();
    SUPPORTED_ENDPOINTS.forEach(endpoints::add);
    response.set("endpoints", endpoints);
    response.put("protocol-version", 1);

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

    // Build delta-rest response
    ObjectNode response = MAPPER.createObjectNode();
    response.put("table-id", stagingTableInfo.getId().toString());
    response.put("table-type", "MANAGED");
    response.put("location", stagingTableInfo.getStagingLocation());

    // Add required protocol (minimal defaults)
    ObjectNode requiredProtocol = MAPPER.createObjectNode();
    requiredProtocol.put("min-reader-version", 1);
    requiredProtocol.put("min-writer-version", 2);
    response.set("required-protocol", requiredProtocol);

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

    try {
      var credentials =
          storageCredentialVendor.vendCredential(
              storageLocation,
              Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));

      return HttpResponse.of(
          HttpStatus.OK,
          MediaType.JSON,
          MAPPER.writeValueAsString(convertCredentialsToResponse(storageLocation, credentials)));
    } catch (Exception e) {
      LOGGER.warn("Could not vend credentials for staging table {}: {}", tableId, e.getMessage());
      // Return empty credentials for POC when no credential provider is configured
      ObjectNode response = MAPPER.createObjectNode();
      response.set("storage-credentials", MAPPER.createArrayNode());
      return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
    }
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
    String tableTypeStr = requestBody.path("table-type").asText("EXTERNAL");
    String formatStr = requestBody.path("data-source-format").asText("DELTA");
    String comment = requestBody.path("comment").asText(null);

    if (name == null || name.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required");
    }
    if (location == null || location.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table location is required");
    }

    // Convert delta-rest columns to UC ColumnInfo
    List<ColumnInfo> columns = new ArrayList<>();
    JsonNode columnsNode = requestBody.path("columns");
    if (columnsNode.isArray()) {
      int position = 0;
      for (JsonNode col : columnsNode) {
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setName(col.path("name").asText());
        // Convert the type field to typeText (handle both primitive and complex types)
        JsonNode typeNode = col.path("type");
        String typeText;
        if (typeNode.isTextual()) {
          typeText = typeNode.asText();
        } else {
          typeText = typeNode.toString();
        }
        columnInfo.setTypeText(typeText);
        columnInfo.setTypeJson(MAPPER.writeValueAsString(col));
        columnInfo.setTypeName(
            io.unitycatalog.server.model.ColumnTypeName.fromValue(mapDeltaTypeToUCType(typeText)));
        columnInfo.setNullable(col.path("nullable").asBoolean(true));
        columnInfo.setPosition(position++);

        // Extract comment from metadata if present
        JsonNode metadata = col.path("metadata");
        if (metadata.has("comment")) {
          columnInfo.setComment(metadata.path("comment").asText());
        }
        columns.add(columnInfo);
      }
    }

    // Convert properties
    Map<String, String> properties = new HashMap<>();
    JsonNode propsNode = requestBody.path("properties");
    if (propsNode.isObject()) {
      propsNode.fields().forEachRemaining(e -> properties.put(e.getKey(), e.getValue().asText()));
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

    // Handle initial commit info if provided
    Long lastCommitVersion = null;
    Long lastCommitTimestamp = null;
    if (requestBody.has("last-commit-version")) {
      lastCommitVersion = requestBody.path("last-commit-version").asLong();
    }
    if (requestBody.has("last-commit-timestamp-ms")) {
      lastCommitTimestamp = requestBody.path("last-commit-timestamp-ms").asLong();
    }

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            buildLoadTableResponse(
                tableInfo, catalog, schema, lastCommitVersion, lastCommitTimestamp)));
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

    // Fetch unbackfilled commits from DeltaCommitRepository
    Long latestTableVersion = null;
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
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Could not fetch commits for table {}: {}", fullName, e.getMessage());
      }
    }

    Map<String, Object> response =
        buildLoadTableResponse(tableInfo, catalog, schema, latestTableVersion, null);
    // Override commits and latest-table-version with actual data
    response.put("commits", commitsList);
    if (latestTableVersion != null) {
      response.put("latest-table-version", latestTableVersion);
    }

    // Add credentials if requested
    if (withCredentials.orElse(false) && tableInfo.getStorageLocation() != null) {
      try {
        NormalizedURL storageLocation = NormalizedURL.from(tableInfo.getStorageLocation());
        var credentials =
            storageCredentialVendor.vendCredential(
                storageLocation,
                Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));
        response.put(
            "storage-credentials",
            convertCredentialsToResponse(storageLocation, credentials).get("storage-credentials"));
      } catch (Exception e) {
        LOGGER.warn("Could not vend credentials for table {}: {}", fullName, e.getMessage());
      }
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
            // For the POC, we use updatedAt as a simple etag
            String expectedEtag = req.path("etag").asText();
            String currentEtag =
                tableInfo.getUpdatedAt() != null ? String.valueOf(tableInfo.getUpdatedAt()) : "0";
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

    // Process updates
    Long lastCommitVersion = null;
    Long lastCommitTimestamp = null;
    JsonNode updates = requestBody.path("updates");
    if (updates.isArray()) {
      for (JsonNode update : updates) {
        String action = update.path("action").asText();
        switch (action) {
          case "set-properties":
            // For POC, log the properties update
            LOGGER.info("set-properties update: {}", update.path("updates"));
            break;
          case "remove-properties":
            LOGGER.info("remove-properties update: {}", update.path("removals"));
            break;
          case "set-protocol":
            LOGGER.info("set-protocol update: {}", update.path("protocol"));
            break;
          case "set-columns":
            LOGGER.info("set-columns update: {}", update.path("columns"));
            break;
          case "set-partition-columns":
            LOGGER.info("set-partition-columns update: {}", update.path("partition-columns"));
            break;
          case "set-table-comment":
            LOGGER.info("set-table-comment update: {}", update.path("comment"));
            break;
          case "set-domain-metadata":
            LOGGER.info("set-domain-metadata update: domain={}", update.path("domain").asText());
            break;
          case "remove-domain-metadata":
            LOGGER.info("remove-domain-metadata update: {}", update.path("domains"));
            break;
          case "add-commit":
            JsonNode commit = update.path("commit");
            LOGGER.info("add-commit update: version={}", commit.path("version").asLong());

            // Post the commit through the existing DeltaCommitRepository
            io.unitycatalog.server.model.DeltaCommitInfo commitInfo =
                new io.unitycatalog.server.model.DeltaCommitInfo()
                    .version(commit.path("version").asLong())
                    .timestamp(commit.path("timestamp").asLong())
                    .fileName(commit.path("file-name").asText())
                    .fileSize(commit.path("file-size").asLong())
                    .fileModificationTimestamp(commit.path("file-modification-timestamp").asLong());

            io.unitycatalog.server.model.DeltaCommit deltaCommit =
                new io.unitycatalog.server.model.DeltaCommit()
                    .tableId(tableInfo.getTableId())
                    .tableUri(tableInfo.getStorageLocation())
                    .commitInfo(commitInfo);

            // Handle uniform metadata if present
            JsonNode uniformNode = update.path("uniform");
            if (!uniformNode.isMissingNode() && uniformNode.has("iceberg")) {
              JsonNode icebergNode = uniformNode.path("iceberg");
              String metadataLoc = icebergNode.path("metadata-location").asText(null);
              io.unitycatalog.server.model.DeltaUniformIceberg icebergInfo =
                  new io.unitycatalog.server.model.DeltaUniformIceberg()
                      .metadataLocation(
                          metadataLoc != null ? java.net.URI.create(metadataLoc) : null)
                      .convertedDeltaVersion(
                          icebergNode.has("converted-delta-version")
                              ? icebergNode.path("converted-delta-version").asLong()
                              : null)
                      .convertedDeltaTimestamp(
                          icebergNode.has("converted-delta-timestamp")
                              ? icebergNode.path("converted-delta-timestamp").asText(null)
                              : null);
              io.unitycatalog.server.model.DeltaUniform ucUniform =
                  new io.unitycatalog.server.model.DeltaUniform().iceberg(icebergInfo);
              deltaCommit.uniform(ucUniform);
            }

            LOGGER.info(
                "Posting commit: tableId={}, tableUri={}, version={}",
                deltaCommit.getTableId(),
                deltaCommit.getTableUri(),
                commitInfo.getVersion());
            deltaCommitRepository.postCommit(deltaCommit);
            lastCommitVersion = commit.path("version").asLong();
            lastCommitTimestamp = commit.path("timestamp").asLong();
            break;
          case "set-latest-backfilled-version":
            long latestPublishedVersion = update.path("latest-published-version").asLong();
            LOGGER.info("set-latest-backfilled-version: version={}", latestPublishedVersion);
            // Delegate to DeltaCommitRepository for backfilling
            io.unitycatalog.server.model.DeltaCommit backfillCommit =
                new io.unitycatalog.server.model.DeltaCommit()
                    .tableId(tableInfo.getTableId())
                    .tableUri(tableInfo.getStorageLocation())
                    .latestBackfilledVersion(latestPublishedVersion);
            deltaCommitRepository.postCommit(backfillCommit);
            break;
          case "update-snapshot-version":
            lastCommitVersion = update.path("last-commit-version").asLong();
            lastCommitTimestamp = update.path("last-commit-timestamp-ms").asLong();
            LOGGER.info(
                "update-snapshot-version: version={}, timestamp={}",
                lastCommitVersion,
                lastCommitTimestamp);
            break;
          default:
            LOGGER.warn("Unknown update action: {}", action);
        }
      }
    }

    // Re-read the table to get updated state
    tableInfo = tableRepository.getTable(fullName);

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        MAPPER.writeValueAsString(
            buildLoadTableResponse(
                tableInfo, catalog, schema, lastCommitVersion, lastCommitTimestamp)));
  }

  @Delete("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public HttpResponse deleteTable(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table) {
    LOGGER.info("Delta REST deleteTable: catalog={}, schema={}, table={}", catalog, schema, table);
    String fullName = catalog + "." + schema + "." + table;
    tableRepository.deleteTable(fullName);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON, "{}");
  }

  @Head("/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}")
  public HttpResponse tableExists(
      @Param("catalog") String catalog,
      @Param("schema") String schema,
      @Param("table") String table) {
    String fullName = catalog + "." + schema + "." + table;
    try {
      tableRepository.getTable(fullName);
      return HttpResponse.of(
          ResponseHeaders.builder(HttpStatus.OK).contentType(MediaType.JSON).build());
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        return HttpResponse.of(ResponseHeaders.of(HttpStatus.NOT_FOUND));
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

    String fullName = catalog + "." + schema + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);

    if (tableInfo.getStorageLocation() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table has no storage location");
    }

    NormalizedURL storageLocation = NormalizedURL.from(tableInfo.getStorageLocation());
    try {
      var credentials =
          storageCredentialVendor.vendCredential(
              storageLocation,
              Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));

      return HttpResponse.of(
          HttpStatus.OK,
          MediaType.JSON,
          MAPPER.writeValueAsString(convertCredentialsToResponse(storageLocation, credentials)));
    } catch (Exception e) {
      LOGGER.warn("Could not vend credentials for table {}: {}", fullName, e.getMessage());
      // Return empty credentials for POC
      ObjectNode response = MAPPER.createObjectNode();
      response.set("storage-credentials", MAPPER.createArrayNode());
      return HttpResponse.of(HttpStatus.OK, MediaType.JSON, MAPPER.writeValueAsString(response));
    }
  }

  // ---- Rename Table ----

  @Post("/v1/catalogs/{catalog}/tables/rename")
  @ProducesJson
  public HttpResponse renameTable(@Param("catalog") String catalog, JsonNode requestBody) {
    // Not implemented for POC
    LOGGER.info("Delta REST renameTable: catalog={}, body={}", catalog, requestBody);
    throw new BaseException(ErrorCode.UNIMPLEMENTED, "Table rename is not yet implemented");
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
          MAPPER.writeValueAsString(convertCredentialsToResponse(storageLocation, credentials)));
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
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  // ---- Helper Methods ----

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
        config.put("aws.access-key-id", aws.getAccessKeyId());
      }
      if (aws.getSecretAccessKey() != null) {
        config.put("aws.secret-access-key", aws.getSecretAccessKey());
      }
      if (aws.getSessionToken() != null) {
        config.put("aws.session-token", aws.getSessionToken());
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
