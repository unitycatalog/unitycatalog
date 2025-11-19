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
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

public class StagingTableRepository {
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private final ServerProperties serverProperties;

  public StagingTableRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  private StagingTableDAO findByStagingLocation(Session session, String stagingLocation) {
    String hql = "FROM StagingTableDAO t WHERE t.stagingLocation = :stagingLocation";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("stagingLocation", stagingLocation);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private void validateIfAlreadyExists(
      Session session, UUID schemaId, String tableName, String stagingLocation) {
    // Check if table by the same name already exists. It's OK if a staging table with the same name
    // already exist.
    TableInfoDAO existingTable =
        repositories.getTableRepository().findBySchemaIdAndName(session, schemaId, tableName);
    if (existingTable != null) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + tableName);
    }
    // Also ensure that no staging table exists at the same location. This is almost impossible as
    // the generated path contains a newly generated random UUID. But still check for it anyway.
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
   * @throws BaseException with ErrorCode.ALREADY_EXISTS if a regular table with the same name
   *     already exists in the schema
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

  /**
   * Commits a staging table by marking it as finalized and associating it with a permanent table.
   *
   * <p>This method is called by TableRepository.createTable when a table is created using a staging
   * table location. It validates that the caller is the owner of the staging table and that the
   * staging table has not been previously committed.
   *
   * @param session the Hibernate session for database operations
   * @param callerId the identifier of the user attempting to commit the staging table
   * @param storageLocation the storage location URI of the staging table to commit
   * @return StagingTableDAO the committed staging table data access object
   * @throws BaseException with ErrorCode.NOT_FOUND if no staging table exists at the specified
   *     location
   * @throws BaseException with ErrorCode.PERMISSION_DENIED if the caller is not the owner of the
   *     staging table
   * @throws BaseException with ErrorCode.FAILED_PRECONDITION if the staging table has already been
   *     committed
   */
  public StagingTableDAO commitStagingTable(
      Session session, String callerId, String storageLocation) {
    serverProperties.checkManagedTableEnabled();
    StagingTableDAO stagingTableDAO = findByStagingLocation(session, storageLocation);
    if (stagingTableDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Staging table not found: " + storageLocation);
    }
    if (!Objects.equals(stagingTableDAO.getCreatedBy(), callerId)) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "User attempts to create table on a staging location without ownership: "
              + storageLocation);
    }
    if (stagingTableDAO.isStageCommitted()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "Staging table already committed: " + storageLocation);
    }

    // Commit the staging table
    Date now = new Date();
    stagingTableDAO.setStageCommitted(true);
    stagingTableDAO.setStageCommittedAt(now);
    stagingTableDAO.setAccessedAt(now);
    session.merge(stagingTableDAO);
    return stagingTableDAO;
  }
}
