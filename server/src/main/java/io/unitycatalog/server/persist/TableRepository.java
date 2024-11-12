package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.*;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
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
    LOGGER.debug("Getting table by id: {}", tableId);
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, UUID.fromString(tableId));
        if (tableInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableId);
        }
        SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, tableInfoDAO.getSchemaId());
        if (schemaInfoDAO == null) {
          throw new BaseException(
              ErrorCode.NOT_FOUND, "Schema not found: " + tableInfoDAO.getSchemaId());
        }
        CatalogInfoDAO catalogInfoDAO =
            session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
        if (catalogInfoDAO == null) {
          throw new BaseException(
              ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
        }
        TableInfo tableInfo = tableInfoDAO.toTableInfo(true);
        tableInfo.setSchemaName(schemaInfoDAO.getName());
        tableInfo.setCatalogName(catalogInfoDAO.getName());
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
    LOGGER.debug("Getting table: {}", fullName);
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

  public TableInfo updateTable(String fullName, UpdateTable updateTable) {
    TableInfo tableInfo = null;
    String callerId = IdentityUtils.findPrincipalEmailAddress();
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
        TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
        if (tableInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableName);
        }

        // Update the columns
        if (updateTable.getColumns() != null) {
          tableInfoDAO.getColumns().forEach(session::remove);
          tableInfoDAO.getColumns().clear();
          session.flush();
          tableInfoDAO.addColumns(ColumnInfoDAO.fromList(updateTable.getColumns()));
          tableInfoDAO
              .getColumns()
              .forEach(
                  c -> {
                    c.setId(UUID.randomUUID());
                    c.setTable(tableInfoDAO);
                  });
        }

        // Update the storage_location
        if (updateTable.getStorageLocation() != null) {
          tableInfoDAO.setUrl(FileUtils.convertRelativePathToURI(updateTable.getStorageLocation()));
        }

        // Update the comment
        if (updateTable.getComment() != null) {
          tableInfoDAO.setComment(updateTable.getComment());
        }

        // Update the props
        if (updateTable.getProperties() != null) {
          PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::remove);
          session.flush();
          PropertyDAO.from(updateTable.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::persist);
        }

        tableInfoDAO.setUpdatedAt(new Date());
        tableInfoDAO.setUpdatedBy(callerId);
        session.merge(tableInfoDAO);

        tableInfo = tableInfoDAO.toTableInfo(true);
        tableInfo.setCatalogName(catalogName);
        tableInfo.setSchemaName(schemaName);
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
        tx.commit();
        return tableInfo;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public TableInfo createTable(CreateTable createTable) {
    ValidationUtils.validateSqlObjectName(createTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    List<ColumnInfo> columnInfos =
        createTable.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());
    Long createTime = System.currentTimeMillis();
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
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId);
    String fullName = getTableFullName(tableInfo);
    LOGGER.debug("Creating table: {}", fullName);

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
        // only external table creation is supported at this time
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
    LOGGER.debug("Finding table by schemaId: {} and name: {}", schemaId, name);
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
