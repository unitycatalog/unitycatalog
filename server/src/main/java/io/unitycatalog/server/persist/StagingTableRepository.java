package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import java.util.Date;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingTableRepository {
  private static final StagingTableRepository INSTANCE = new StagingTableRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(StagingTableRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  private StagingTableRepository() {}

  public static StagingTableRepository getInstance() {
    return INSTANCE;
  }

  public StagingTableInfo getStagingTableById(String stagingTableId) {
    try (Session session = SESSION_FACTORY.openSession()) {
      StagingTableDAO stagingTableDAO =
          session.get(StagingTableDAO.class, UUID.fromString(stagingTableId));
      if (stagingTableDAO == null) {
        throw new BaseException(ErrorCode.NOT_FOUND, "Staging table not found: " + stagingTableId);
      }
      SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, stagingTableDAO.getSchemaId());
      if (schemaInfoDAO == null) {
        throw new BaseException(
            ErrorCode.NOT_FOUND, "Schema not found: " + stagingTableDAO.getSchemaId());
      }
      CatalogInfoDAO catalogInfoDAO =
          session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
      if (catalogInfoDAO == null) {
        throw new BaseException(
            ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
      }
      StagingTableInfo stagingTableInfo = stagingTableDAO.toStagingTableInfo();
      stagingTableInfo.catalogName(catalogInfoDAO.getName());
      stagingTableInfo.schemaName(schemaInfoDAO.getName());
      return stagingTableInfo;
    }
  }

  private StagingTableDAO findStagingTableByName(Session session, UUID schemaId, String tableName) {
    String hql = "FROM StagingTableDAO t WHERE t.schemaId = :schemaId AND t.name = :tableName";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("tableName", tableName);
    return query.uniqueResult(); // Returns null if no result is found
  }

  public StagingTableDAO findByStagingLocation(Session session, String stagingLocation) {
    String hql = "FROM StagingTableDAO t WHERE t.stagingLocation = :stagingLocation";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("stagingLocation", stagingLocation);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private void validateIfAlreadyExists(
      Session session, UUID schemaId, String tableName, String stagingLocation) {
    // check if staging table or table by the same name already exists
    // Also ensure that no staging table exists at the same location
    StagingTableDAO existingStagingTable = findStagingTableByName(session, schemaId, tableName);
    if (existingStagingTable != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists: " + tableName);
    }
    TableInfoDAO existingTable =
        TABLE_REPOSITORY.findBySchemaIdAndName(session, schemaId, tableName);
    if (existingTable != null) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + tableName);
    }
    StagingTableDAO existingStagingTableAtLocation =
        findByStagingLocation(session, stagingLocation);
    if (existingStagingTableAtLocation != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists: " + stagingLocation);
    }
  }

  public StagingTableInfo createStagingTable(CreateStagingTable createStagingTable) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        UUID schemaId =
            RepositoryUtils.getSchemaId(
                session, createStagingTable.getCatalogName(), createStagingTable.getSchemaName());
        UUID stagingTableId = UUID.randomUUID();
        String stagingLocation = FileUtils.createTableDirectory(stagingTableId.toString());

        validateIfAlreadyExists(session, schemaId, createStagingTable.getName(), stagingLocation);

        StagingTableDAO stagingTableDAO = new StagingTableDAO();
        stagingTableDAO.setId(stagingTableId);
        stagingTableDAO.setSchemaId(schemaId);
        stagingTableDAO.setName(createStagingTable.getName());
        stagingTableDAO.setStagingLocation(stagingLocation);
        stagingTableDAO.setDefaultFields();
        session.persist(stagingTableDAO);
        tx.commit();
        return stagingTableDAO.toStagingTableInfo();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public StagingTableDAO commitStagingTable(Session session, String storageLocation) {
    StagingTableDAO stagingTableDAO = findByStagingLocation(session, storageLocation);
    if (stagingTableDAO != null) {
      // Commit the staging table
      if (stagingTableDAO.isStageCommitted()) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION, "Staging table already committed: " + storageLocation);
      }
      stagingTableDAO.setStageCommitted(true);
      stagingTableDAO.setStageCommittedAt(new Date());
      stagingTableDAO.setAccessedAt(new Date());
      session.persist(stagingTableDAO);
      return stagingTableDAO;
    } else {
      throw new BaseException(ErrorCode.NOT_FOUND, "Staging table not found: " + storageLocation);
    }
  }
}
