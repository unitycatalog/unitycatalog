package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
   * @return the standardized URI string of the storage location
   * @throws BaseException with ErrorCode.NOT_FOUND if neither a table nor staging table is found
   *     with the given ID
   */
  public String getStorageLocationForTableOrStagingTable(UUID tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of table by id: {}", tableId);
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO != null) {
            return FileOperations.toStandardizedURIString(tableInfoDAO.getUrl());
          }

          LOGGER.debug("Getting storage location of staging table by id: {}", tableId);
          StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
          if (stagingTableDAO != null) {
            return FileOperations.toStandardizedURIString(stagingTableDAO.getStagingLocation());
          }
          throw new BaseException(
              ErrorCode.NOT_FOUND, "Neither table nor staging table found with id: " + tableId);
        },
        "Failed to get storage location of table or staging table",
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
   * @throws BaseException with ErrorCode.NOT_FOUND if neither a table nor staging table is found
   *     with the given ID, or if the associated schema is not found
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
                  ErrorCode.NOT_FOUND, "Neither table nor staging table found with id: " + tableId);
            }
            schemaId = stagingTableDAO.getSchemaId();
          }

          SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
          if (schemaInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found with id: " + schemaId);
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
            throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + fullName);
          }
          TableInfo tableInfo = tableInfoDAO.toTableInfo(true, catalogName, schemaName);
          RepositoryUtils.attachProperties(
              tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
          return tableInfo;
        },
        "Failed to get table",
        /* readOnly = */ true);
  }

  public String getTableUniformMetadataLocation(
      Session session, String catalogName, String schemaName, String tableName) {
    TableInfoDAO dao = findTable(session, catalogName, schemaName, tableName);
    return dao.getUniformIcebergMetadataLocation();
  }

  private TableInfoDAO findTable(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaId(session, catalogName, schemaName);
    return findBySchemaIdAndName(session, schemaId, tableName);
  }

  public TableInfo createTable(CreateTable createTable) {
    ValidationUtils.validateSqlObjectName(createTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    List<ColumnInfo> columnInfos =
        createTable.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());
    Long createTime = System.currentTimeMillis();
    String fullName = getTableFullName(createTable);
    LOGGER.debug("Creating table: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = createTable.getCatalogName();
          String schemaName = createTable.getSchemaName();
          UUID schemaId =
              repositories.getSchemaRepository().getSchemaId(session, catalogName, schemaName);

          // Check if table already exists
          TableInfoDAO existingTable =
              findBySchemaIdAndName(session, schemaId, createTable.getName());
          if (existingTable != null) {
            throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + fullName);
          }
          TableType tableType = Objects.requireNonNull(createTable.getTableType());
          // The table ID will either be a new random one or the id of staging table, depending
          // on the type of table to create.
          String tableID;
          if (tableType == TableType.EXTERNAL) {
            tableID = UUID.randomUUID().toString();
          } else if (tableType == TableType.MANAGED) {
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
                    .commitStagingTable(session, callerId, createTable.getStorageLocation());
            tableID = stagingTableDAO.getId().toString();
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
                  .storageLocation(
                      FileOperations.toStandardizedURIString(createTable.getStorageLocation()))
                  .tableId(tableID);

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
          return tableInfo;
        },
        "Error creating table: " + fullName,
        /* readOnly = */ false);
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
              repositories.getSchemaRepository().getSchemaId(session, catalogName, schemaName);
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
              repositories.getSchemaRepository().getSchemaId(session, catalogName, schemaName);
          deleteTable(session, schemaId, tableName);
          return null;
        },
        "Failed to delete table",
        /* readOnly = */ false);
  }

  public void deleteTable(Session session, UUID schemaId, String tableName) {
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableName);
    }
    if (TableType.MANAGED.getValue().equals(tableInfoDAO.getType())) {
      try {
        fileOperations.deleteDirectory(tableInfoDAO.getUrl());
      } catch (Throwable e) {
        LOGGER.error("Error deleting table directory: {}", tableInfoDAO.getUrl(), e);
      }
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
