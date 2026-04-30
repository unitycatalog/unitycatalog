package io.unitycatalog.server.persist;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

import io.unitycatalog.server.delta.model.DeltaCommit;
import io.unitycatalog.server.delta.model.LoadTableResponse;
import io.unitycatalog.server.delta.model.StructType;
import io.unitycatalog.server.delta.model.TableMetadata;
import io.unitycatalog.server.delta.model.UniformMetadata;
import io.unitycatalog.server.delta.model.UniformMetadataIceberg;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DependencyList;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private final FileOperations fileOperations;
  private final ServerProperties serverProperties;
  private static final PagedListingHelper<TableInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(TableInfoDAO.class);

  public TableRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.fileOperations = repositories.getFileOperations();
    this.serverProperties = serverProperties;
  }

  /**
   * Retrieves the storage location for a table or staging table by its ID. First attempts to find a
   * regular table with the given ID, then falls back to searching for a staging table if no regular
   * table is found. NOTE: This function is specially needed by generateTemporaryTableCredential
   * during the short window when a staging table is just created and the initial data is being
   * written but before the actual table is already created. Reading of a staging table is not a
   * common supplemental of an actual table but only a special case.
   *
   * @param tableId the ID of the table or staging table
   * @return the normalized URL of the storage location
   * @throws BaseException with ErrorCode.TABLE_NOT_FOUND if neither a table nor staging table is
   *     found with the given ID
   */
  public NormalizedURL getStorageLocationForTableOrStagingTable(UUID tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of table by id: {}", tableId);
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO != null) {
            return NormalizedURL.from(tableInfoDAO.getUrl());
          }

          LOGGER.debug("Getting storage location of staging table by id: {}", tableId);
          StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
          if (stagingTableDAO != null) {
            return NormalizedURL.from(stagingTableDAO.getStagingLocation());
          }
          throw new BaseException(
              ErrorCode.TABLE_NOT_FOUND,
              "Neither table nor staging table found with id: " + tableId);
        },
        "Failed to get storage location of table or staging table",
        /* readOnly = */ true);
  }

  /**
   * Looks up the storage location for a regular table by its three-part name. Only reads what the
   * caller actually needs (the storage URL) rather than hydrating the full {@link TableInfo} with
   * columns and properties. Accepts the three parts separately so callers don't have to round-trip
   * them through a dotted string that the repo would immediately split again.
   *
   * @throws BaseException with ErrorCode.TABLE_NOT_FOUND if no table exists at the given name.
   */
  public NormalizedURL getTableStorageLocation(String catalog, String schema, String table) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO dao = findTable(session, catalog, schema, table);
          if (dao == null) {
            throw new BaseException(
                ErrorCode.TABLE_NOT_FOUND,
                "Table not found: " + catalog + "." + schema + "." + table);
          }
          return NormalizedURL.from(dao.getUrl());
        },
        "Failed to get storage location of table " + catalog + "." + schema + "." + table,
        /* readOnly = */ true);
  }

  /**
   * Looks up the storage location for a staging table by ID. Unlike {@link
   * #getStorageLocationForTableOrStagingTable}, this rejects regular table UUIDs so endpoints
   * scoped to staging tables don't silently accept regular-table inputs.
   *
   * @throws BaseException with ErrorCode.TABLE_NOT_FOUND if no staging table exists with this ID.
   */
  public NormalizedURL getStagingTableStorageLocation(UUID stagingTableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of staging table by id: {}", stagingTableId);
          StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, stagingTableId);
          if (stagingTableDAO == null) {
            throw new BaseException(
                ErrorCode.TABLE_NOT_FOUND, "Staging table not found with id: " + stagingTableId);
          }
          return NormalizedURL.from(stagingTableDAO.getStagingLocation());
        },
        "Failed to get storage location of staging table",
        /* readOnly = */ true);
  }

  /**
   * Retrieves the schema ID and catalog ID for a table or staging table by its ID. First attempts
   * to get IDs associated with a regular table with the given ID, then falls back to searching for
   * a staging table if no regular table is found. NOTE: Similar to
   * getStorageLocationForTableOrStagingTable, this function is specially needed by KeyMapper during
   * authorization of generateTemporaryTableCredential. Reading of a staging table is not a common
   * supplemental of an actual table but only a special case.
   *
   * @param tableId the UUID of the table or staging table
   * @return a Pair containing the catalog ID (left) and schema ID (right)
   * @throws BaseException with ErrorCode.TABLE_NOT_FOUND if neither a table nor staging table is
   *     found with the given ID
   * @throws BaseException with ErrorCode.SCHEMA_NOT_FOUND if the associated schema is not found
   */
  public Pair<UUID, UUID> getCatalogSchemaIdsByTableOrStagingTableId(UUID tableId) {
    LOGGER.debug("Getting catalog&schema id by table or staging table id: {}", tableId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);

          UUID schemaId;
          if (tableInfoDAO != null) {
            schemaId = tableInfoDAO.getSchemaId();
          } else {
            // Table not found, try to find a staging table instead
            StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
            if (stagingTableDAO == null) {
              throw new BaseException(
                  ErrorCode.TABLE_NOT_FOUND,
                  "Neither table nor staging table found with id: " + tableId);
            }
            schemaId = stagingTableDAO.getSchemaId();
          }

          SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
          if (schemaInfoDAO == null) {
            throw new BaseException(
                ErrorCode.SCHEMA_NOT_FOUND, "Schema not found with id: " + schemaId);
          }

          return Pair.of(schemaInfoDAO.getCatalogId(), schemaId);
        },
        "Failed to get table or staging table by ID",
        /* readOnly = */ true);
  }

  public TableInfo getTable(String fullName) {
    LOGGER.debug("Getting table: {}", fullName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String tableName = parts[2];
          TableInfoDAO tableInfoDAO = findTable(session, catalogName, schemaName, tableName);
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.TABLE_NOT_FOUND, "Table not found: " + fullName);
          }
          TableInfo tableInfo = tableInfoDAO.toTableInfo(true, catalogName, schemaName);
          RepositoryUtils.attachProperties(
              tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
          RepositoryUtils.attachDependencies(
              tableInfo, tableInfoDAO, session, repositories.getDependencyRepository());
          return tableInfo;
        },
        "Failed to get table",
        /* readOnly = */ true);
  }

  /**
   * Load a table for the Delta REST Catalog API in a single REPEATABLE_READ transaction.
   *
   * <p>Returns a {@link LoadTableResponse} containing:
   *
   * <ul>
   *   <li>Table metadata (format, type, location, columns, partition columns, properties)
   *   <li>Unbackfilled commits (managed Delta tables only; empty for external tables)
   *   <li>Uniform metadata (Iceberg location/version if present)
   * </ul>
   *
   * <p>Column parsing is best-effort: corrupt typeJson data yields an empty schema rather than
   * failing the entire response.
   */
  public LoadTableResponse loadTableForDelta(String catalog, String schema, String table) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO dao = findTable(session, catalog, schema, table);
          if (dao == null) {
            throw new BaseException(
                ErrorCode.TABLE_NOT_FOUND,
                "Table not found: " + catalog + "." + schema + "." + table);
          }

          // Guard non-Delta entries (metric views, parquet/csv/etc. tables) before they reach
          // `buildTableMetadata`. The downstream code calls `NormalizedURL.normalize(dao.getUrl())`
          // (throws "Path cannot be null or empty" when the row has no storage location, e.g.
          // metric views) and `DataSourceFormat.fromValue(dao.getDataSourceFormat())` (throws
          // IllegalArgumentException on null), both of which surface as misleading errors to a
          // Delta REST client that simply asked for a name that happens to live in the same
          // schema. Sibling guard to `DeltaCommitRepository.validateTable`.
          if (dao.getDataSourceFormat() == null
              || !DataSourceFormat.DELTA.toString().equals(dao.getDataSourceFormat())) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                "Table is not a Delta table: " + catalog + "." + schema + "." + table);
          }

          TableMetadata metadata = buildTableMetadata(session, dao, catalog, schema, table);

          LoadTableResponse response = new LoadTableResponse();
          response.setMetadata(metadata);

          // Commits (managed Delta tables only)
          if (TableType.MANAGED.toString().equals(dao.getType())
              && DataSourceFormat.DELTA.toString().equals(dao.getDataSourceFormat())) {
            populateCommitsForDelta(
                response, repositories.getDeltaCommitRepository(), session, dao.getId());
          }

          populateUniformMetadata(response, dao);

          return response;
        },
        "Failed to load table",
        /* readOnly = */ true,
        Optional.of(TRANSACTION_REPEATABLE_READ));
  }

  private TableMetadata buildTableMetadata(
      Session session, TableInfoDAO dao, String catalog, String schema, String table) {
    TableMetadata metadata = new TableMetadata();
    Long updatedAt = dao.getUpdatedAt() != null ? dao.getUpdatedAt().getTime() : null;
    // Normal case: etag keyed on the table's last update time. If updatedAt is missing
    // (shouldn't happen for a persisted table, but defensively handled), fall back to the
    // table UUID so the etag stays unique per table rather than collapsing to "etag-null"
    // across rows.
    metadata.setEtag(updatedAt != null ? "etag-" + updatedAt : "etag-" + dao.getId());
    metadata.setDataSourceFormat(toDeltaFormat(dao.getDataSourceFormat()));
    metadata.setTableType(toDeltaTableType(dao.getType()));
    metadata.setTableUuid(dao.getId());
    metadata.setLocation(NormalizedURL.normalize(dao.getUrl()));
    metadata.setCreatedTime(dao.getCreatedAt() != null ? dao.getCreatedAt().getTime() : null);
    metadata.setUpdatedTime(updatedAt);
    metadata.setSecurableType(io.unitycatalog.server.delta.model.SecurableType.TABLE);

    // Columns -- best-effort; corrupt data should not fail the entire response
    StructType emptySchema = new StructType().fields(List.of());
    List<ColumnInfo> cols = List.of();
    try {
      cols = ColumnInfoDAO.toList(dao.getColumns());
      if (cols != null && !cols.isEmpty()) {
        metadata.setColumns(
            new StructType().fields(cols.stream().map(ColumnUtils::toStructField).toList()));
      } else {
        metadata.setColumns(emptySchema);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to parse columns for table {}.{}.{}, returning empty schema",
          catalog,
          schema,
          table,
          e);
      metadata.setColumns(emptySchema);
    }

    populatePartitionColumns(metadata, cols, catalog, schema, table);

    List<PropertyDAO> propDAOs =
        PropertyRepository.findProperties(session, dao.getId(), Constants.TABLE);
    Map<String, String> props = PropertyDAO.toMap(propDAOs);
    metadata.setProperties(props);

    // last-commit-version and last-commit-timestamp track only metadata-changing commits
    // (delta.lastUpdateVersion / delta.lastCommitTimestamp) and are written by the commit path.
    // They are distinct from CommitQueryResult.latestTableVersion, which advances on every
    // commit including data-only ones. Reading from table properties preserves that distinction.
    parseLongProperty(props, TableProperties.LAST_UPDATE_VERSION)
        .ifPresent(metadata::setLastCommitVersion);
    parseLongProperty(props, TableProperties.LAST_COMMIT_TIMESTAMP)
        .ifPresent(metadata::setLastCommitTimestampMs);

    return metadata;
  }

  private static void populatePartitionColumns(
      TableMetadata metadata, List<ColumnInfo> cols, String catalog, String schema, String table) {
    List<ColumnInfo> partitionInfos =
        cols.stream()
            .filter(c -> c.getPartitionIndex() != null)
            .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
            .toList();
    for (int i = 0; i < partitionInfos.size(); i++) {
      if (partitionInfos.get(i).getPartitionIndex() != i) {
        // Non-contiguous indices mean the persisted partition spec is corrupt. Emit an empty
        // partition list rather than a possibly-partial one the client can't reconcile.
        LOGGER.warn(
            "Table {}.{}.{} has invalid partition indices, expected {} but got {}; "
                + "emitting empty partition columns",
            catalog,
            schema,
            table,
            i,
            partitionInfos.get(i).getPartitionIndex());
        metadata.setPartitionColumns(List.of());
        return;
      }
    }
    metadata.setPartitionColumns(partitionInfos.stream().map(ColumnInfo::getName).toList());
  }

  private static Optional<Long> parseLongProperty(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(value));
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid long value for property {}: {}", key, value);
      return Optional.empty();
    }
  }

  /** Populate unbackfilled commits from DeltaCommitRepository into the response. */
  private static void populateCommitsForDelta(
      LoadTableResponse response, DeltaCommitRepository commitRepo, Session session, UUID tableId) {
    DeltaCommitRepository.CommitQueryResult result =
        commitRepo.getUnbackfilledCommits(session, tableId);
    response.setLatestTableVersion(result.latestTableVersion());

    List<DeltaCommit> commits =
        result.commits().stream()
            .map(
                c ->
                    new DeltaCommit()
                        .version(c.getCommitVersion())
                        .timestamp(c.getCommitTimestamp().getTime())
                        .fileName(c.getCommitFilename())
                        .fileSize(c.getCommitFilesize())
                        .fileModificationTimestamp(
                            c.getCommitFileModificationTimestamp().getTime()))
            .toList();
    response.setCommits(commits);
  }

  private static void populateUniformMetadata(LoadTableResponse response, TableInfoDAO dao) {
    String uniformLocation = dao.getUniformIcebergMetadataLocation();
    if (uniformLocation == null) {
      return;
    }
    UniformMetadataIceberg iceberg = new UniformMetadataIceberg().metadataLocation(uniformLocation);
    if (dao.getUniformIcebergConvertedDeltaVersion() != null) {
      iceberg.convertedDeltaVersion(dao.getUniformIcebergConvertedDeltaVersion());
    }
    if (dao.getUniformIcebergConvertedDeltaTimestamp() != null) {
      iceberg.convertedDeltaTimestamp(dao.getUniformIcebergConvertedDeltaTimestamp().getTime());
    }
    response.setUniform(new UniformMetadata().iceberg(iceberg));
  }

  // Delta model enum converters (avoid FQ names for types that
  // conflict with io.unitycatalog.server.model.*)
  private static io.unitycatalog.server.delta.model.DataSourceFormat toDeltaFormat(String value) {
    return io.unitycatalog.server.delta.model.DataSourceFormat.fromValue(value);
  }

  private static io.unitycatalog.server.delta.model.TableType toDeltaTableType(String value) {
    return io.unitycatalog.server.delta.model.TableType.fromValue(value);
  }

  public String getTableUniformMetadataLocation(
      Session session, String catalogName, String schemaName, String tableName) {
    TableInfoDAO dao = findTable(session, catalogName, schemaName, tableName);
    return dao.getUniformIcebergMetadataLocation();
  }

  private TableInfoDAO findTable(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return findBySchemaIdAndName(session, schemaId, tableName);
  }

  public TableInfo createTable(CreateTable createTable) {
    ValidationUtils.validateSqlObjectName(createTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    List<ColumnInfo> columnInfos =
        createTable.getColumns().stream()
            .map(
                c -> {
                  ColumnUtils.validateTypeJson(c);
                  return c.typeText(c.getTypeText().toLowerCase(Locale.ROOT));
                })
            .toList();
    Long createTime = System.currentTimeMillis();
    String fullName = getTableFullName(createTable);
    LOGGER.debug("Creating table: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = createTable.getCatalogName();
          String schemaName = createTable.getSchemaName();
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);

          // Check if table already exists
          TableInfoDAO existingTable =
              findBySchemaIdAndName(session, schemaId, createTable.getName());
          if (existingTable != null) {
            throw new BaseException(
                ErrorCode.TABLE_ALREADY_EXISTS, "Table already exists: " + fullName);
          }
          TableType tableType = Objects.requireNonNull(createTable.getTableType());
          // `tableUUID` is the table's primary key. The shape is uniform across the three
          // creatable branches (external, managed, metric view); the only divergence is the
          // source of the UUID (random for external/metric-view, staging-table id for managed).
          // The string form is generated exactly once below at `tableInfo.tableId(...)`.
          UUID tableUUID;
          NormalizedURL storageLocation;
          if (tableType == TableType.EXTERNAL) {
            storageLocation = NormalizedURL.from(createTable.getStorageLocation());
            ExternalLocationUtils.validateNotOverlapWithManagedStorage(session, storageLocation);
            tableUUID = UUID.randomUUID();
          } else if (tableType == TableType.MANAGED) {
            storageLocation = NormalizedURL.from(createTable.getStorageLocation());
            serverProperties.checkManagedTableEnabled();
            if (createTable.getDataSourceFormat() != DataSourceFormat.DELTA) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Managed table creation is only supported for Delta format.");
            }
            // Find and commit staging table with the same staging location
            StagingTableDAO stagingTableDAO =
                repositories
                    .getStagingTableRepository()
                    .commitStagingTable(session, callerId, storageLocation);
            tableUUID = stagingTableDAO.getId();
          } else if (tableType == TableType.METRIC_VIEW) {
            storageLocation = null;
            validateMetricView(createTable);
            tableUUID = UUID.randomUUID();
          } else if (tableType == TableType.STREAMING_TABLE) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT, "STREAMING TABLE creation is not supported yet.");
          } else if (tableType == TableType.MATERIALIZED_VIEW) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT, "MATERIALIZED VIEW creation is not supported yet.");
          } else {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                "Unrecognized table type " + createTable.getTableType());
          }
          TableInfo tableInfo =
              new TableInfo()
                  .name(createTable.getName())
                  .catalogName(createTable.getCatalogName())
                  .schemaName(createTable.getSchemaName())
                  .tableType(createTable.getTableType())
                  .dataSourceFormat(createTable.getDataSourceFormat())
                  .columns(columnInfos)
                  .comment(createTable.getComment())
                  .properties(createTable.getProperties())
                  .owner(callerId)
                  .createdAt(createTime)
                  .createdBy(callerId)
                  .updatedAt(createTime)
                  .updatedBy(callerId)
                  .storageLocation(storageLocation != null ? storageLocation.toString() : null)
                  .viewDefinition(createTable.getViewDefinition())
                  .tableId(tableUUID.toString());

          TableInfoDAO tableInfoDAO = TableInfoDAO.from(tableInfo, schemaId);
          // create columns
          tableInfoDAO
              .getColumns()
              .forEach(
                  c -> {
                    c.setId(UUID.randomUUID());
                    c.setTable(tableInfoDAO);
                  });
          // create properties
          PropertyDAO.from(tableInfo.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::persist);
          session.persist(tableInfoDAO);
          if (tableType == TableType.METRIC_VIEW) {
            DependencyDAO.DependentType dependentType = DependencyDAO.DependentType.TABLE;
            List<DependencyDAO> depDAOs =
                createTable.getViewDependencies().getDependencies().stream()
                    .map(dep -> DependencyDAO.from(dep, tableUUID, dependentType))
                    .collect(Collectors.toList());
            repositories
                .getDependencyRepository()
                .createDependencies(session, tableUUID, dependentType, depDAOs);
          }
          return tableInfo;
        },
        "Error creating table: " + fullName,
        /* readOnly = */ false);
  }

  private static void validateMetricView(CreateTable createTable) {
    if (createTable.getViewDefinition() == null || createTable.getViewDefinition().isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "view_definition is required for metric view");
    }
    DependencyList viewDeps = createTable.getViewDependencies();
    if (viewDeps == null || viewDeps.getDependencies() == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "view_dependencies is required for metric view");
    }
    if (viewDeps.getDependencies().isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "view_dependencies must contain at least one entry for metric view");
    }
  }

  public TableInfoDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql = "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding table by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private String getTableFullName(CreateTable createTable) {
    return createTable.getCatalogName()
        + "."
        + createTable.getSchemaName()
        + "."
        + createTable.getName();
  }

  /**
   * Return the list of tables in ascending order of table name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @param omitProperties
   * @param omitColumns
   * @return
   */
  public ListTablesResponse listTables(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          return listTables(
              session,
              schemaId,
              catalogName,
              schemaName,
              maxResults,
              pageToken,
              omitProperties,
              omitColumns);
        },
        "Failed to list tables",
        /* readOnly = */ true);
  }

  public ListTablesResponse listTables(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    List<TableInfoDAO> tableInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(tableInfoDAOList, maxResults);
    List<TableInfo> result = new ArrayList<>();
    for (TableInfoDAO tableInfoDAO : tableInfoDAOList) {
      TableInfo tableInfo = tableInfoDAO.toTableInfo(!omitColumns, catalogName, schemaName);
      if (!omitProperties) {
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
      }
      RepositoryUtils.attachDependencies(
          tableInfo, tableInfoDAO, session, repositories.getDependencyRepository());
      result.add(tableInfo);
    }
    return new ListTablesResponse().tables(result).nextPageToken(nextPageToken);
  }

  public void deleteTable(String fullName) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String tableName = parts[2];
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          deleteTable(session, schemaId, tableName);
          return null;
        },
        "Failed to delete table",
        /* readOnly = */ false);
  }

  public void deleteTable(Session session, UUID schemaId, String tableName) {
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.TABLE_NOT_FOUND, "Table not found: " + tableName);
    }
    if (TableType.MANAGED.getValue().equals(tableInfoDAO.getType())) {
      try {
        FileOperations.deleteDirectory(NormalizedURL.from(tableInfoDAO.getUrl()));
      } catch (Throwable e) {
        LOGGER.error("Error deleting table directory: {}", tableInfoDAO.getUrl(), e);
      }
      repositories
          .getDeltaCommitRepository()
          .permanentlyDeleteTableCommits(session, tableInfoDAO.getId());
    }
    if (TableType.METRIC_VIEW.getValue().equals(tableInfoDAO.getType())) {
      repositories
          .getDependencyRepository()
          .deleteDependencies(session, tableInfoDAO.getId(), DependencyDAO.DependentType.TABLE);
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
