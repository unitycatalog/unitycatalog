package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
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
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  public static final Integer DEFAULT_PAGE_SIZE = 100;

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
    String schemaId = getSchemaId(session, catalogName, schemaName);
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
            .createdAt(System.currentTimeMillis());
    String fullName = getTableFullName(tableInfo);
    LOGGER.debug("Creating table: " + fullName);

    Transaction tx;
    try (Session session = SESSION_FACTORY.openSession()) {
      String catalogName = tableInfo.getCatalogName();
      String schemaName = tableInfo.getSchemaName();
      String schemaId = getSchemaId(session, catalogName, schemaName);
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
        tableInfoDAO.setSchemaId(UUID.fromString(schemaId));
        // create columns
        tableInfoDAO.getColumns().forEach(c -> c.setTable(tableInfoDAO));
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
      throw new BaseException(ErrorCode.INTERNAL, "Error creating table: " + fullName, e);
    }
    return tableInfo;
  }

  public TableInfoDAO findBySchemaIdAndName(Session session, String schemaId, String name) {
    String hql = "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", UUID.fromString(schemaId));
    query.setParameter("name", name);
    LOGGER.debug("Finding table by schemaId: " + schemaId + " and name: " + name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private String getTableFullName(TableInfo tableInfo) {
    return tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();
  }

  public String getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo = SCHEMA_REPOSITORY.getSchemaDAO(session, catalogName, schemaName);
    if (schemaInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
    return schemaInfo.getId().toString();
  }

  public static Date convertMillisToDate(Optional<String> millisString) {
    if (millisString.isEmpty()) {
      return null;
    }
    try {
      long millis = Long.parseLong(millisString.get());
      return new Date(millis);
    } catch (NumberFormatException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unable to interpret page token: " + millisString);
    }
  }

  public static String getNextPageToken(List<TableInfoDAO> tables, Integer pageSize) {
    if (tables == null || tables.isEmpty() || tables.size() < pageSize) {
      return null;
    }
    // Assuming the last item in the list is the least recent based on the query
    long time = tables.get(tables.size() - 1).getCreatedAt().getTime();
    if (tables.get(tables.size() - 1).getUpdatedAt() != null)
      time = tables.get(tables.size() - 1).getUpdatedAt().getTime();
    return String.valueOf(time);
  }

  public static Integer getPageSize(Optional<Integer> maxResults) {
    return maxResults
        .filter(x -> (x > 0))
        .map(x -> Math.min(x, DEFAULT_PAGE_SIZE))
        .orElse(DEFAULT_PAGE_SIZE);
  }

  /**
   * Return the most recently updated tables first in descending order of updated time
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param nextPageToken
   * @param omitProperties
   * @param omitColumns
   * @return
   */
  public ListTablesResponse listTables(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> nextPageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        String schemaId = getSchemaId(session, catalogName, schemaName);
        ListTablesResponse response =
            listTables(
                session,
                UUID.fromString(schemaId),
                catalogName,
                schemaName,
                maxResults,
                nextPageToken,
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
      Optional<String> nextPageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    List<TableInfo> result = new ArrayList<>();
    String returnNextPageToken;
    if (maxResults.isPresent() && maxResults.get() < 0) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "maxResults must be greater than or equal to 0");
    }
    Integer pageSize = getPageSize(maxResults);
    String hql =
        "FROM TableInfoDAO t WHERE t.schemaId = :schemaId and "
            + "(t.updatedAt < :pageToken OR :pageToken is null) order by t.updatedAt desc";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("pageToken", convertMillisToDate(nextPageToken));
    query.setMaxResults(pageSize);
    List<TableInfoDAO> tableInfoDAOList = query.list();
    returnNextPageToken = getNextPageToken(tableInfoDAOList, pageSize);
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
    return new ListTablesResponse().tables(result).nextPageToken(returnNextPageToken);
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
        String schemaId = getSchemaId(session, catalogName, schemaName);
        deleteTable(session, UUID.fromString(schemaId), tableName);
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
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId.toString(), tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableName);
    }
    if (TableType.MANAGED.getValue().equals(tableInfoDAO.getType())) {
      try {
        FileUtils.deleteDirectory(tableInfoDAO.getUrl());
      } catch (Throwable e) {
        LOGGER.error("Error deleting table directory: " + tableInfoDAO.getUrl());
      }
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
