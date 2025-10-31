package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingTableRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(StagingTableRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private final ServerProperties serverProperties;

  public StagingTableRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  private StagingTableDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql = "FROM StagingTableDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding staging table by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private StagingTableDAO findByStagingLocation(Session session, String stagingLocation) {
    String hql = "FROM StagingTableDAO t WHERE t.stagingLocation = :stagingLocation";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("stagingLocation", stagingLocation);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private void validateIfAlreadyExists(
      Session session, UUID schemaId, String tableName, String stagingLocation) {
    // check if staging table or table by the same name already exists
    // Also ensure that no staging table exists at the same location
    StagingTableDAO existingStagingTable = findBySchemaIdAndName(session, schemaId, tableName);
    if (existingStagingTable != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists: " + tableName);
    }
    TableInfoDAO existingTable =
        repositories.getTableRepository().findBySchemaIdAndName(session, schemaId, tableName);
    if (existingTable != null) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + tableName);
    }
    StagingTableDAO existingStagingTableAtLocation =
        findByStagingLocation(session, stagingLocation);
    if (existingStagingTableAtLocation != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists at: " + stagingLocation);
    }
  }

  private String getTableFullName(CreateStagingTable createStagingTable) {
    return createStagingTable.getCatalogName()
        + "."
        + createStagingTable.getSchemaName()
        + "."
        + createStagingTable.getName();
  }

  /**
   * Creates a new staging table in the specified schema.
   *
   * <p>A staging table is a temporary location used during managed table creation. It provides a
   * staging area where data can be written before finalizing the table creation. The staging
   * location is automatically generated and must be committed before the table becomes permanent.
   *
   * @param createStagingTable the request containing catalog name, schema name, and table name
   * @return StagingTableInfo containing the created staging table details including the staging
   *     location
   * @throws BaseException with ErrorCode.ALREADY_EXISTS if a staging table or regular table with
   *     the same name already exists in the schema
   * @throws BaseException with ErrorCode.NOT_FOUND if the specified catalog or schema does not
   *     exist
   */
  public StagingTableInfo createStagingTable(CreateStagingTable createStagingTable) {
    serverProperties.checkManagedTableEnabled();
    ValidationUtils.validateSqlObjectName(createStagingTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID stagingTableId = UUID.randomUUID();
    String stagingLocation =
        repositories.getFileOperations().createTableDirectory(stagingTableId.toString());

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaId(
                      session,
                      createStagingTable.getCatalogName(),
                      createStagingTable.getSchemaName());
          validateIfAlreadyExists(session, schemaId, createStagingTable.getName(), stagingLocation);

          StagingTableDAO stagingTableDAO = new StagingTableDAO();
          stagingTableDAO.setDefaultFields();
          stagingTableDAO.setId(stagingTableId);
          stagingTableDAO.setSchemaId(schemaId);
          stagingTableDAO.setName(createStagingTable.getName());
          stagingTableDAO.setStagingLocation(stagingLocation);
          stagingTableDAO.setCreatedBy(callerId);
          session.persist(stagingTableDAO);
          return stagingTableDAO.toStagingTableInfo(
              createStagingTable.getCatalogName(), createStagingTable.getSchemaName());
        },
        "Error creating table: " + getTableFullName(createStagingTable),
        /* readOnly = */ false);
  }
}
