package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRepository {
  private static final TableRepository INSTANCE = new TableRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final PagedListingHelper<TableInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(TableInfoDAO.class);

  private TableRepository() {}

  public static TableRepository getInstance() {
    return INSTANCE;
  }

  public TableInfo getTableById(String tableId) {
    LOGGER.debug("Getting table by id: " + tableId);
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, UUID.fromString(tableId));
        if (tableInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableId);
        }
        TableInfo tableInfo = tableInfoDAO.toTableInfo(true);
        tx.commit();
        return tableInfo;
      } catch (Exception e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public TableInfo getTable(String fullName) {
    LOGGER.debug("Getting table: " + fullName);
    TableInfo tableInfo = null;
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
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
        tableInfo = tableInfoDAO.toTableInfo(true);
        tableInfo.setCatalogName(catalogName);
        tableInfo.setSchemaName(schemaName);
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
        tx.commit();
        return tableInfo;
      } catch (Exception e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public String getTableUniformMetadataLocation(
      Session session, String catalogName, String schemaName, String tableName) {
    TableInfoDAO dao = findTable(session, catalogName, schemaName, tableName);
    return dao.getUniformIcebergMetadataLocation();
  }

  private TableInfoDAO findTable(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId = getSchemaId(session, catalogName, schemaName);
    return findBySchemaIdAndName(session, schemaId, tableName);
  }

  public TableInfo createTable(CreateTable createTable) {
    ValidationUtils.validateSqlObjectName(createTable.getName());
    List<ColumnInfo> columnInfos =
        createTable.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());
    TableInfo tableInfo =
        new TableInfo()
            .tableId(UUID.randomUUID().toString())
            .name(createTable.getName())
            .catalogName(createTable.getCatalogName())
            .schemaName(createTable.getSchemaName())
            .tableType(createTable.getTableType())
            .dataSourceFormat(createTable.getDataSourceFormat())
            .columns(columnInfos)
            .storageLocation(FileUtils.convertRelativePathToURI(createTable.getStorageLocation()))
            .comment(createTable.getComment())
            .properties(createTable.getProperties())
            .uniformIcebergMetadataLocation(createTable.getUniformIcebergMetadataLocation())
            .createdAt(System.currentTimeMillis());
    String fullName = getTableFullName(tableInfo);
    LOGGER.debug("Creating table: " + fullName);

    Transaction tx;
    try (Session session = SESSION_FACTORY.openSession()) {
      String catalogName = tableInfo.getCatalogName();
      String schemaName = tableInfo.getSchemaName();
      UUID schemaId = getSchemaId(session, catalogName, schemaName);
      tx = session.beginTransaction();

      try {
        // Check if table already exists
        TableInfoDAO existingTable = findBySchemaIdAndName(session, schemaId, tableInfo.getName());
        if (existingTable != null) {
          throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + fullName);
        }
        if (TableType.MANAGED.equals(tableInfo.getTableType())) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "MANAGED table creation is not supported yet.");
        }
        // assuming external table
        if (tableInfo.getStorageLocation() == null) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "Storage location is required for external table");
        }
        TableInfoDAO tableInfoDAO = TableInfoDAO.from(tableInfo);
        tableInfoDAO.setSchemaId(schemaId);
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
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    } catch (RuntimeException e) {
      if (e instanceof BaseException) {
        throw e;
      }
      throw new BaseException(
          ErrorCode.INTERNAL, "Error creating table: " + fullName + ". " + e.getMessage(), e);
    }
    return tableInfo;
  }

  public TableInfoDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql = "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding table by schemaId: " + schemaId + " and name: " + name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private String getTableFullName(TableInfo tableInfo) {
    return tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();
  }

  public UUID getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo = SCHEMA_REPOSITORY.getSchemaDAO(session, catalogName, schemaName);
    if (schemaInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
    return schemaInfo.getId();
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
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
        ListTablesResponse response =
            listTables(
                session,
                schemaId,
                catalogName,
                schemaName,
                maxResults,
                pageToken,
                omitProperties,
                omitColumns);
        tx.commit();
        return response;
      } catch (Exception e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
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
      TableInfo tableInfo = tableInfoDAO.toTableInfo(!omitColumns);
      if (!omitProperties) {
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
      }
      tableInfo.setCatalogName(catalogName);
      tableInfo.setSchemaName(schemaName);
      result.add(tableInfo);
    }
    return new ListTablesResponse().tables(result).nextPageToken(nextPageToken);
  }

  public void deleteTable(String fullName) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      String[] parts = fullName.split("\\.");
      if (parts.length != 3) {
        throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
      }
      String catalogName = parts[0];
      String schemaName = parts[1];
      String tableName = parts[2];
      try {
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
        deleteTable(session, schemaId, tableName);
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public void deleteTable(Session session, UUID schemaId, String tableName) {
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableName);
    }
    if (TableType.MANAGED.getValue().equals(tableInfoDAO.getType())) {
      try {
        FileUtils.deleteDirectory(tableInfoDAO.getUrl());
      } catch (Throwable e) {
        LOGGER.error("Error deleting table directory: {}", tableInfoDAO.getUrl(), e);
      }
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
