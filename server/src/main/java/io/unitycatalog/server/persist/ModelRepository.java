package io.unitycatalog.server.persist;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateModelVersion;
import io.unitycatalog.server.model.CreateRegisteredModel;
import io.unitycatalog.server.model.FinalizeModelVersion;
import io.unitycatalog.server.model.ListModelVersionsResponse;
import io.unitycatalog.server.model.ListRegisteredModelsResponse;
import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.ModelVersionStatus;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.UpdateModelVersion;
import io.unitycatalog.server.model.UpdateRegisteredModel;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModelRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private final ServerProperties serverProperties;
  private static final PagedListingHelper<RegisteredModelInfoDAO> REGISTERED_MODEL_LISTING_HELPER =
      new PagedListingHelper<>(RegisteredModelInfoDAO.class);

  public ModelRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  /** **************** DAO retrieval methods ***************** */
  public RegisteredModelInfoDAO getRegisteredModelDao(Session session, UUID schemaId, String name) {
    String hql = "FROM RegisteredModelInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<RegisteredModelInfoDAO> query = session.createQuery(hql, RegisteredModelInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    query.setMaxResults(1);
    LOGGER.info("Finding registered model by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  public RegisteredModelInfoDAO getRegisteredModelDaoOrThrow(
      Session session, UUID schemaId, String name) {
    RegisteredModelInfoDAO existingRegisteredModelDao =
        getRegisteredModelDao(session, schemaId, name);
    if (existingRegisteredModelDao == null) {
      throw registeredModelNotFound(name);
    }
    return existingRegisteredModelDao;
  }

  private static BaseException registeredModelNotFound(String name) {
    return new BaseException(ErrorCode.NOT_FOUND, "Registered model not found: " + name);
  }

  private static BaseException modelVersionNotFound(String fullVersionName) {
    return new BaseException(ErrorCode.NOT_FOUND, "Model version not found: " + fullVersionName);
  }

  private static String describeVersion(String fullName, long version) {
    return fullName + "/" + version;
  }

  public List<RegisteredModelInfoDAO> getAllRegisteredModelsDao(
      Session session, Optional<String> token, Optional<Integer> maxResults) {
    if (maxResults.isPresent() && maxResults.get() < 0) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "maxResults must be greater than or equal to 0");
    }
    UUID tokenToUse = new UUID(0, 0);
    if (token.isPresent()) {
      try {
        tokenToUse = UUID.fromString(token.get());
      } catch (IllegalArgumentException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Invalid page token received: " + token.get());
      }
    }
    String hql = "FROM RegisteredModelInfoDAO t WHERE t.id > :token ORDER BY t.id ASC";
    Query<RegisteredModelInfoDAO> query = session.createQuery(hql, RegisteredModelInfoDAO.class);
    query.setParameter("token", tokenToUse);
    query.setMaxResults(REGISTERED_MODEL_LISTING_HELPER.getPageSize(maxResults));
    return query.getResultList(); // Returns null if no result is found
  }

  public ModelVersionInfoDAO getModelVersionDao(Session session, UUID modelId, Long version) {
    String hql =
        "FROM ModelVersionInfoDAO t WHERE t.registeredModelId = :registeredModelId AND t.version ="
            + " :version";
    Query<ModelVersionInfoDAO> query = session.createQuery(hql, ModelVersionInfoDAO.class);
    query.setParameter("registeredModelId", modelId);
    query.setParameter("version", version.toString());
    query.setMaxResults(1);
    LOGGER.info("Finding model version by registeredModelId: {} and version: {}", modelId, version);
    return query.uniqueResult(); // Returns null if no result is found
  }

  public ModelVersionInfoDAO getModelVersionDaoOrThrow(
      Session session, UUID modelId, String fullName, Long version) {
    ModelVersionInfoDAO modelVersionInfoDAO = getModelVersionDao(session, modelId, version);
    if (modelVersionInfoDAO == null) {
      throw new BaseException(
          ErrorCode.NOT_FOUND, "Model version not found: " + fullName + "/" + version);
    }
    return modelVersionInfoDAO;
  }

  public ModelVersionInfoDAO getModelVersionDaoOrThrow(
      Session session, UUID schemaId, String fullName, String registeredModelName, Long version) {
    RegisteredModelInfoDAO rmInfoDao =
        getRegisteredModelDaoOrThrow(session, schemaId, registeredModelName);
    ModelVersionInfoDAO modelVersionInfoDAO =
        getModelVersionDaoOrThrow(session, rmInfoDao.getId(), fullName, version);
    return modelVersionInfoDAO;
  }

  public List<ModelVersionInfoDAO> getModelVersionsDao(
      Session session, UUID registeredModelId, String token, int maxResults) {
    String hql =
        "FROM ModelVersionInfoDAO t WHERE t.registeredModelId = :registeredModelId AND t.version >"
            + " :token ORDER BY t.version ASC";
    Query<ModelVersionInfoDAO> query = session.createQuery(hql, ModelVersionInfoDAO.class);
    query.setParameter("registeredModelId", registeredModelId);
    query.setParameter("token", Long.parseLong(token));
    query.setMaxResults(maxResults);
    LOGGER.info("Finding model versions by registeredModelId: {}", registeredModelId);
    return query.getResultList(); // Returns null if no result is found
  }

  /** **************** ModelRepository convenience methods ***************** */
  private String getRegisteredModelFullName(RegisteredModelInfo registeredModelInfo) {
    return getRegisteredModelFullName(
        registeredModelInfo.getCatalogName(),
        registeredModelInfo.getSchemaName(),
        registeredModelInfo.getName());
  }

  private String getRegisteredModelFullName(
      String catalogName, String schemaName, String modelName) {
    return catalogName + "." + schemaName + "." + modelName;
  }

  public String getNextPageToken(List<ModelVersionInfoDAO> entities, Optional<Integer> maxResults) {
    if (entities == null
        || entities.isEmpty()
        || entities.size() < PagedListingHelper.getPageSize(maxResults)) {
      return null;
    }
    // return the last version
    return entities.get(entities.size() - 1).getVersion().toString();
  }

  private String getNextPageTokenById(
      List<RegisteredModelInfoDAO> entities, Optional<Integer> maxResults) {
    if (entities == null
        || entities.isEmpty()
        || entities.size() < PagedListingHelper.getPageSize(maxResults)) {
      return null;
    }
    // return the last id
    return entities.get(entities.size() - 1).getId().toString();
  }

  /** **************** Registered Model handlers ***************** */
  public RegisteredModelInfo getRegisteredModel(String fullName) {
    LOGGER.info("Getting registered model: {}", fullName);
    RegisteredModelInfo registeredModelInfo = null;
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
        String catalogName = parts[0];
        String schemaName = parts[1];
        String registeredModelName = parts[2];
        RegisteredModelInfoDAO registeredModelInfoDAO =
            findRegisteredModel(session, catalogName, schemaName, registeredModelName);
        if (registeredModelInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Registered model not found: " + fullName);
        }
        registeredModelInfo = registeredModelInfoDAO.toRegisteredModelInfo();
        registeredModelInfo.setCatalogName(catalogName);
        registeredModelInfo.setSchemaName(schemaName);
        registeredModelInfo.setFullName(getRegisteredModelFullName(registeredModelInfo));
        tx.commit();
        return registeredModelInfo;
      } catch (Exception e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  private RegisteredModelInfoDAO findRegisteredModel(
      Session session, String catalogName, String schemaName, String registeredModelName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return getRegisteredModelDao(session, schemaId, registeredModelName);
  }

  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel) {
    ValidationUtils.validateSqlObjectName(createRegisteredModel.getName());
    long createTime = System.currentTimeMillis();
    UUID modelId = UUID.randomUUID();
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    RegisteredModelInfo registeredModelInfo =
        new RegisteredModelInfo()
            .id(modelId.toString())
            .name(createRegisteredModel.getName())
            .catalogName(createRegisteredModel.getCatalogName())
            .schemaName(createRegisteredModel.getSchemaName())
            .comment(createRegisteredModel.getComment())
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId);
    String fullName = getRegisteredModelFullName(registeredModelInfo);
    registeredModelInfo.setFullName(fullName);
    LOGGER.info("Creating Registered Model: {}", fullName);

    Transaction tx;
    try (Session session = sessionFactory.openSession()) {
      tx = session.beginTransaction();
      String catalogName = registeredModelInfo.getCatalogName();
      String schemaName = registeredModelInfo.getSchemaName();
      RepositoryUtils.CatalogAndSchemaDao catalogAndSchemaDao =
          RepositoryUtils.getCatalogAndSchemaDaoOrThrow(session, catalogName, schemaName);
      NormalizedURL parentStorageLocation =
          ExternalLocationUtils.getManagedStorageLocation(
              catalogAndSchemaDao, this::getDefaultModelsStorageRoot);
      NormalizedURL storageLocation =
          ExternalLocationUtils.getManagedLocationForModel(parentStorageLocation, modelId);
      SchemaInfoDAO schemaInfoDAO = catalogAndSchemaDao.schemaInfoDAO();
      try {
        repositories
            .getSchemaRepository()
            .lockSchemaForChildCreate(session, schemaInfoDAO.getId(), catalogName, schemaName);
        // Check if registered model already exists
        RegisteredModelInfoDAO existingRegisteredModel =
            getRegisteredModelDao(session, schemaInfoDAO.getId(), registeredModelInfo.getName());
        if (existingRegisteredModel != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS, "Registered model already exists: " + fullName);
        }
        registeredModelInfo.setStorageLocation(storageLocation.toString());
        RegisteredModelInfoDAO registeredModelInfoDAO =
            RegisteredModelInfoDAO.from(registeredModelInfo);
        registeredModelInfoDAO.setSchemaId(schemaInfoDAO.getId());
        registeredModelInfoDAO.setMaxVersionNumber(0L);
        session.persist(registeredModelInfoDAO);
        FileOperations.createStorageLocationDir(storageLocation);
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          try {
            // For now, never delete.  We will implement a soft delete later.
            // UriUtils.deleteStorageLocationPath(storageLocation);
          } catch (Exception deleteErr) {
            LOGGER.error(
                "Unable to delete storage location {} during rollback: {}",
                storageLocation,
                deleteErr.getMessage());
          }
          tx.rollback();
        }
        throw e;
      }
    } catch (RuntimeException e) {
      if (e instanceof BaseException) {
        throw e;
      }
      throw new BaseException(
          ErrorCode.INTERNAL, "Error creating registered model: " + fullName, e);
    }
    return registeredModelInfo;
  }

  public ListRegisteredModelsResponse listRegisteredModels(
      Optional<String> catalogName,
      Optional<String> schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    catalogName = catalogName.filter(name -> !name.isEmpty());
    schemaName = schemaName.filter(name -> !name.isEmpty());
    if (catalogName.isPresent() && schemaName.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Cannot specify catalog w/o schema for list registered models.");
    }
    if (catalogName.isEmpty() && schemaName.isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Cannot specify schema w/o catalog for list registered models.");
    }
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        ListRegisteredModelsResponse response = new ListRegisteredModelsResponse();
        if (catalogName.isEmpty() || schemaName.isEmpty()) {
          // Run the custom query to pull all models back from all catalogs/schemas
          LOGGER.info("Listing all registered models in the metastore.");
          List<RegisteredModelInfoDAO> registeredModelInfoDAOList =
              getAllRegisteredModelsDao(session, pageToken, maxResults);
          // Model names are not unique across the metastore, so this branch pages by id, not name.
          String nextPageToken = getNextPageTokenById(registeredModelInfoDAOList, maxResults);
          List<RegisteredModelInfo> result = new ArrayList<>();
          for (RegisteredModelInfoDAO registeredModelInfoDAO : registeredModelInfoDAOList) {
            RepositoryUtils.CatalogAndSchemaNames names =
                RepositoryUtils.getCatalogAndSchemaNames(
                    session, registeredModelInfoDAO.getSchemaId());

            RegisteredModelInfo registeredModelInfo =
                registeredModelInfoDAO.toRegisteredModelInfo();
            registeredModelInfo.setCatalogName(names.catalogName());
            registeredModelInfo.setSchemaName(names.schemaName());
            registeredModelInfo.setFullName(getRegisteredModelFullName(registeredModelInfo));
            result.add(registeredModelInfo);
          }
          return new ListRegisteredModelsResponse()
              .registeredModels(result)
              .nextPageToken(nextPageToken);
        } else {
          LOGGER.info("Listing registered models in {}.{}", catalogName.get(), schemaName.get());
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName.get(), schemaName.get());
          response =
              listRegisteredModels(
                  session, schemaId, catalogName.get(), schemaName.get(), maxResults, pageToken);
        }
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

  public ListRegisteredModelsResponse listRegisteredModels(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    List<RegisteredModelInfoDAO> registeredModelInfoDAOList =
        REGISTERED_MODEL_LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken =
        REGISTERED_MODEL_LISTING_HELPER.getNextPageToken(registeredModelInfoDAOList, maxResults);
    List<RegisteredModelInfo> result = new ArrayList<>();
    for (RegisteredModelInfoDAO registeredModelInfoDAO : registeredModelInfoDAOList) {
      RegisteredModelInfo registeredModelInfo = registeredModelInfoDAO.toRegisteredModelInfo();
      registeredModelInfo.setCatalogName(catalogName);
      registeredModelInfo.setSchemaName(schemaName);
      registeredModelInfo.setFullName(getRegisteredModelFullName(registeredModelInfo));
      result.add(registeredModelInfo);
    }
    return new ListRegisteredModelsResponse().registeredModels(result).nextPageToken(nextPageToken);
  }

  public RegisteredModelInfo updateRegisteredModel(
      String fullName, UpdateRegisteredModel updateRegisteredModel) {
    if (updateRegisteredModel.getNewName() != null) {
      ValidationUtils.validateSqlObjectName(updateRegisteredModel.getNewName());
    }
    if (fullName == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No three tier full name specified.");
    }
    if (updateRegisteredModel.getNewName() == null && updateRegisteredModel.getComment() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No updated fields defined.");
    }

    LOGGER.info("Updating Registered Model: {}", fullName);
    RegisteredModelInfo registeredModelInfo;
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    Transaction tx;
    try (Session session = sessionFactory.openSession()) {
      String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      tx = session.beginTransaction();
      try {
        // Verify that the new model name does not already exist in the database
        if (updateRegisteredModel.getNewName() != null) {
          String newFullName =
              getRegisteredModelFullName(
                  catalogName, schemaName, updateRegisteredModel.getNewName());
          RegisteredModelInfoDAO newRegisteredModelInfoDAO =
              findRegisteredModel(
                  session, catalogName, schemaName, updateRegisteredModel.getNewName());
          if (newRegisteredModelInfoDAO != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "Registered model already exists: " + newFullName);
          }
        }
        // Get the record from the database
        RegisteredModelInfoDAO origRegisteredModelInfoDAO =
            findRegisteredModel(session, catalogName, schemaName, registeredModelName);
        if (origRegisteredModelInfoDAO == null) {
          throw registeredModelNotFound(fullName);
        }
        Map<String, Object> key = Map.of("name", registeredModelName);
        RepositoryUtils.lockByKey(
            session,
            RegisteredModelInfoDAO.class,
            origRegisteredModelInfoDAO.getId(),
            key,
            "registered model " + fullName,
            () -> registeredModelNotFound(fullName));
        Map<String, Object> changes = new HashMap<>();
        if (updateRegisteredModel.getNewName() != null) {
          changes.put("name", updateRegisteredModel.getNewName());
        }
        if (updateRegisteredModel.getComment() != null) {
          changes.put("comment", updateRegisteredModel.getComment());
        }
        changes.put("updatedAt", new Date());
        changes.put("updatedBy", callerId);
        RepositoryUtils.updateByKey(
            session,
            origRegisteredModelInfoDAO,
            key,
            changes,
            () -> registeredModelNotFound(fullName));
        registeredModelInfo = origRegisteredModelInfoDAO.toRegisteredModelInfo();
        registeredModelInfo.setCatalogName(catalogName);
        registeredModelInfo.setSchemaName(schemaName);
        registeredModelInfo.setFullName(getRegisteredModelFullName(registeredModelInfo));
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
          ErrorCode.INTERNAL, "Error updating registered model: " + fullName, e);
    }
    return registeredModelInfo;
  }

  public void deleteRegisteredModel(String fullName, boolean force) {
    LOGGER.info("Deleting Registered Model: {}", fullName);
    try (Session session = sessionFactory.openSession()) {
      // READ COMMITTED, so that the reads after the model row's lock see a version created while
      // the delete waited for it, as a repeatable-read snapshot would not.
      TransactionManager.setIsolationLevel(session, TRANSACTION_READ_COMMITTED);
      Transaction tx = session.beginTransaction();
      String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      try {
        UUID schemaId =
            repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
        RegisteredModelInfoDAO found =
            getRegisteredModelDaoOrThrow(session, schemaId, registeredModelName);
        deleteRegisteredModel(
            session,
            found.getId(),
            fullName,
            force,
            () -> registeredModelNotFound(registeredModelName));
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  /**
   * Deletes the registered model {@code modelId} named by {@code fullName}; {@code notFound} is
   * thrown if no such row exists, for example because it was renamed or dropped since the caller
   * found it. With {@code force}, a version that vanishes while the model is deleted fails the
   * delete closed.
   */
  void deleteRegisteredModel(
      Session session,
      UUID modelId,
      String fullName,
      boolean force,
      Supplier<BaseException> notFound) {
    String registeredModelName = RepositoryUtils.parseRegisteredModelFullName(fullName)[2];
    Map<String, Object> key = Map.of("name", registeredModelName);
    RegisteredModelInfoDAO registeredModelInfoDAO =
        RepositoryUtils.lockByKey(
            session,
            RegisteredModelInfoDAO.class,
            modelId,
            key,
            "registered model " + registeredModelName,
            notFound);
    if (force) {
      // Remove all model versions, a page at a time until none is left.
      Supplier<BaseException> versionVanished =
          () -> RepositoryUtils.concurrentModification("registered model " + fullName);
      for (List<ModelVersionInfoDAO> versions = nextVersions(session, modelId);
          !versions.isEmpty();
          versions = nextVersions(session, modelId)) {
        for (ModelVersionInfoDAO version : versions) {
          Map<String, Object> versionKey = versionKey(version);
          RepositoryUtils.lockByKey(
              session,
              ModelVersionInfoDAO.class,
              version.getId(),
              versionKey,
              "model version " + fullName + "/" + version.getVersion(),
              versionVanished);
          RepositoryUtils.deleteByKey(session, version, versionKey, versionVanished);
        }
      }
    } else {
      // Check if model versions exist and throw with unable to delete if they exist
      List<ModelVersionInfoDAO> versionList =
          getModelVersionsDao(session, registeredModelInfoDAO.getId(), "0", 1);
      if (versionList.size() > 0) {
        throw new BaseException(
            ErrorCode.ABORTED,
            "Unable to delete a registered model with existing model versions: "
                + registeredModelName);
      }
    }
    // Versions live under the model directory, so one task covers a forced model deletion.
    repositories
        .getStorageCleanupTaskRepository()
        .create(
            session,
            ManagedResourceType.REGISTERED_MODEL,
            registeredModelInfoDAO.getId(),
            registeredModelInfoDAO.getName(),
            registeredModelInfoDAO.getUrl());
    RepositoryUtils.deleteByKey(session, registeredModelInfoDAO, key, notFound);
  }

  private List<ModelVersionInfoDAO> nextVersions(Session session, UUID modelId) {
    return getModelVersionsDao(session, modelId, "0", PagedListingHelper.DEFAULT_PAGE_SIZE);
  }

  /** **************** Model version handlers ***************** */
  public ModelVersionInfo getModelVersion(String fullName, long version) {
    LOGGER.info("Getting model version: {}/{}", fullName, version);
    ModelVersionInfo modelVersionInfo = null;
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
        String catalogName = parts[0];
        String schemaName = parts[1];
        String registeredModelName = parts[2];
        RegisteredModelInfoDAO registeredModelInfoDAO =
            findRegisteredModel(session, catalogName, schemaName, registeredModelName);
        if (registeredModelInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Registered model not found: " + fullName);
        }
        ModelVersionInfoDAO modelVersionDao =
            getModelVersionDao(session, registeredModelInfoDAO.getId(), version);
        if (modelVersionDao == null) {
          throw new BaseException(
              ErrorCode.NOT_FOUND, "Model version not found: " + fullName + "/" + version);
        }
        modelVersionInfo = modelVersionDao.toModelVersionInfo();
        modelVersionInfo.setModelName(registeredModelName);
        modelVersionInfo.setCatalogName(catalogName);
        modelVersionInfo.setSchemaName(schemaName);
        tx.commit();
        return modelVersionInfo;
      } catch (Exception e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public ModelVersionInfo createModelVersion(CreateModelVersion createModelVersion) {
    long createTime = System.currentTimeMillis();
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID modelVersionId = UUID.randomUUID();
    String catalogName = createModelVersion.getCatalogName();
    String schemaName = createModelVersion.getSchemaName();
    String modelName = createModelVersion.getModelName();
    ModelVersionInfo modelVersionInfo =
        new ModelVersionInfo()
            .id(modelVersionId.toString())
            .modelName(createModelVersion.getModelName())
            .catalogName(createModelVersion.getCatalogName())
            .schemaName(createModelVersion.getSchemaName())
            .runId(createModelVersion.getRunId())
            .source(createModelVersion.getSource())
            .status(ModelVersionStatus.PENDING_REGISTRATION)
            .comment(createModelVersion.getComment())
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId);
    String registeredModelFullName = getRegisteredModelFullName(catalogName, schemaName, modelName);
    LOGGER.info("Creating Registered Model: {}", registeredModelFullName);

    Transaction tx;
    try (Session session = sessionFactory.openSession()) {
      tx = session.beginTransaction();
      UUID schemaId =
          repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
      NormalizedURL storageLocation = null;
      try {
        // Check if registered model already exists
        RegisteredModelInfoDAO found = getRegisteredModelDaoOrThrow(session, schemaId, modelName);
        // Lock the model, so concurrent creates number their versions in turn.
        Map<String, Object> modelKey = Map.of("name", modelName);
        RegisteredModelInfoDAO existingRegisteredModel =
            RepositoryUtils.lockByKey(
                session,
                RegisteredModelInfoDAO.class,
                found.getId(),
                modelKey,
                "registered model " + modelName,
                () -> registeredModelNotFound(modelName));
        if (existingRegisteredModel.getMaxVersionNumber() == null
            || existingRegisteredModel.getMaxVersionNumber() < 0) {
          throw new BaseException(
              ErrorCode.OUT_OF_RANGE,
              "Registered model has invalid max model version: "
                  + existingRegisteredModel.getMaxVersionNumber());
        }
        UUID modelId = existingRegisteredModel.getId();
        Long version = existingRegisteredModel.getMaxVersionNumber() + 1;
        storageLocation =
            ExternalLocationUtils.getManagedLocationForModelVersion(
                NormalizedURL.from(existingRegisteredModel.getUrl()), modelVersionId);
        modelVersionInfo.setVersion(version);
        modelVersionInfo.setStorageLocation(storageLocation.toString());
        ModelVersionInfoDAO modelVersionInfoDAO = ModelVersionInfoDAO.from(modelVersionInfo);
        modelVersionInfoDAO.setRegisteredModelId(modelId);
        session.persist(modelVersionInfoDAO);
        FileOperations.createStorageLocationDir(storageLocation);
        // update the registered model
        RepositoryUtils.updateByKey(
            session,
            existingRegisteredModel,
            modelKey,
            Map.of("maxVersionNumber", version),
            () -> registeredModelNotFound(modelName));
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          try {
            // For now, never delete.  We will implement a soft delete later.
            // UriUtils.deleteStorageLocationPath(storageLocation);
          } catch (Exception deleteErr) {
            LOGGER.error(
                "Unable to delete storage location {} during rollback: {}",
                storageLocation,
                deleteErr.getMessage());
          }
          tx.rollback();
        }
        throw e;
      }
    } catch (RuntimeException e) {
      if (e instanceof BaseException) {
        throw e;
      }
      throw new BaseException(
          ErrorCode.INTERNAL,
          "Error creating model version for model: " + registeredModelFullName,
          e);
    }
    return modelVersionInfo;
  }

  public ListModelVersionsResponse listModelVersions(
      String registeredModelFullName, Optional<Integer> maxResults, Optional<String> pageToken) {
    LOGGER.info("Listing model versions in {}", registeredModelFullName);
    if (maxResults.isPresent() && maxResults.get() < 0) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "maxResults must be greater than or equal to 0");
    }
    if (pageToken.isPresent()) {
      try {
        Long.parseLong(pageToken.get());
      } catch (NumberFormatException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Invalid page token received: " + pageToken.get());
      }
    }
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        // Check if registered model already exists
        String[] parts = RepositoryUtils.parseRegisteredModelFullName(registeredModelFullName);
        String catalogName = parts[0];
        String schemaName = parts[1];
        String registeredModelName = parts[2];
        UUID schemaId =
            repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
        RegisteredModelInfoDAO existingRegisteredModel =
            getRegisteredModelDaoOrThrow(session, schemaId, registeredModelName);
        UUID registeredModelId = existingRegisteredModel.getId();
        List<ModelVersionInfoDAO> modelVersions =
            getModelVersionsDao(
                session,
                registeredModelId,
                pageToken.orElse("0"),
                PagedListingHelper.getPageSize(maxResults));
        String nextPageToken = getNextPageToken(modelVersions, maxResults);
        List<ModelVersionInfo> modelVersionInfoList = new ArrayList<ModelVersionInfo>();
        if (modelVersions != null) {
          for (ModelVersionInfoDAO curDao : modelVersions) {
            ModelVersionInfo curInfo = curDao.toModelVersionInfo();
            curInfo.setCatalogName(catalogName);
            curInfo.setSchemaName(schemaName);
            curInfo.setModelName(registeredModelName);
            modelVersionInfoList.add(curInfo);
          }
        }
        ListModelVersionsResponse response =
            new ListModelVersionsResponse()
                .modelVersions(modelVersionInfoList)
                .nextPageToken(nextPageToken);
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

  public ModelVersionInfo updateModelVersion(
      String fullName, Long version, UpdateModelVersion updateModelVersion) {
    if (fullName == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No model specified.");
    }
    if (version == null || version < 1) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "No valid model version specified: " + version);
    }
    if (updateModelVersion.getComment() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No updated fields defined.");
    }

    LOGGER.info("Updating Model Version: {}/{}", fullName, version);
    ModelVersionInfo modelVersionInfo;
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    Transaction tx;
    try (Session session = sessionFactory.openSession()) {
      String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      tx = session.beginTransaction();
      try {
        // Get the registered model record from the database
        UUID schemaId =
            repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
        // Get the model version record from the database
        ModelVersionInfoDAO origModelVersionInfoDAO =
            getModelVersionDaoOrThrow(session, schemaId, fullName, registeredModelName, version);
        Map<String, Object> key = versionKey(origModelVersionInfoDAO);
        lockModelVersion(session, origModelVersionInfoDAO, key, fullName);
        Map<String, Object> changes = new HashMap<>();
        changes.put("comment", updateModelVersion.getComment());
        changes.put("updatedAt", new Date());
        changes.put("updatedBy", callerId);
        RepositoryUtils.updateByKey(
            session,
            origModelVersionInfoDAO,
            key,
            changes,
            () -> modelVersionNotFound(describeVersion(fullName, version)));
        modelVersionInfo = origModelVersionInfoDAO.toModelVersionInfo();
        modelVersionInfo.setCatalogName(catalogName);
        modelVersionInfo.setSchemaName(schemaName);
        modelVersionInfo.setModelName(registeredModelName);
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
          ErrorCode.INTERNAL, "Error updating model version: " + fullName + "/" + version, e);
    }
    return modelVersionInfo;
  }

  public void deleteModelVersion(String fullName, Long version) {
    LOGGER.info("Deleting model version: {}/{}", fullName, version);
    String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
    String catalogName = parts[0];
    String schemaName = parts[1];
    String registeredModelName = parts[2];
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        UUID schemaId =
            repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
        RegisteredModelInfoDAO existingRegisteredModel =
            getRegisteredModelDaoOrThrow(session, schemaId, registeredModelName);
        deleteModelVersion(session, existingRegisteredModel.getId(), fullName, version);
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public void deleteModelVersion(
      Session session, UUID registeredModelId, String fullName, long version) {
    String fullVersionName = describeVersion(fullName, version);
    ModelVersionInfoDAO found = getModelVersionDao(session, registeredModelId, version);
    if (found == null) {
      throw modelVersionNotFound(fullVersionName);
    }
    Map<String, Object> key = versionKey(found);
    ModelVersionInfoDAO modelVersionInfoDAO = lockModelVersion(session, found, key, fullName);
    repositories
        .getStorageCleanupTaskRepository()
        .create(
            session,
            ManagedResourceType.MODEL_VERSION,
            modelVersionInfoDAO.getId(),
            modelVersionInfoDAO.getVersion().toString(),
            modelVersionInfoDAO.getUrl());
    RepositoryUtils.deleteByKey(
        session, modelVersionInfoDAO, key, () -> modelVersionNotFound(fullVersionName));
  }

  /** The model version's key: its model's id plus the version number the request named. */
  private static Map<String, Object> versionKey(ModelVersionInfoDAO version) {
    return Map.of(
        "registeredModelId", version.getRegisteredModelId(), "version", version.getVersion());
  }

  private ModelVersionInfoDAO lockModelVersion(
      Session session, ModelVersionInfoDAO version, Map<String, Object> key, String fullName) {
    String fullVersionName = describeVersion(fullName, version.getVersion());
    return RepositoryUtils.lockByKey(
        session,
        ModelVersionInfoDAO.class,
        version.getId(),
        key,
        "model version " + fullVersionName,
        () -> modelVersionNotFound(fullVersionName));
  }

  public ModelVersionInfo finalizeModelVersion(FinalizeModelVersion finalizeModelVersion) {
    if (finalizeModelVersion.getFullName() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No three tier full name specified.");
    }
    if (finalizeModelVersion.getVersion() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No version specified.");
    }

    String fullName = finalizeModelVersion.getFullName();
    Long version = finalizeModelVersion.getVersion();
    LOGGER.info("Finalize Model Version: {}/{}", fullName, version);
    ModelVersionInfo modelVersionInfo;
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    Transaction tx;
    try (Session session = sessionFactory.openSession()) {
      String[] parts = RepositoryUtils.parseRegisteredModelFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      tx = session.beginTransaction();
      try {
        // Get the registered model record from the database
        UUID schemaId =
            repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
        ModelVersionInfoDAO origModelVersionInfoDAO =
            getModelVersionDaoOrThrow(session, schemaId, fullName, registeredModelName, version);

        Map<String, Object> key = versionKey(origModelVersionInfoDAO);
        lockModelVersion(session, origModelVersionInfoDAO, key, fullName);
        // Check the creator here so the check and status update use the same stored version.
        if (serverProperties.isAuthorizationEnabled()
            && (callerId == null || !callerId.equals(origModelVersionInfoDAO.getCreatedBy()))) {
          throw new BaseException(
              ErrorCode.PERMISSION_DENIED, "Only model version creator can finalize model version");
        }
        if (ModelVersionStatus.valueOf(origModelVersionInfoDAO.getStatus())
            != ModelVersionStatus.PENDING_REGISTRATION) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Model version not in a pending registration state: " + fullName + "/" + version);
        }
        Map<String, Object> changes = new HashMap<>();
        changes.put("status", ModelVersionStatus.READY.toString());
        changes.put("updatedAt", new Date());
        changes.put("updatedBy", callerId);
        RepositoryUtils.updateByKey(
            session,
            origModelVersionInfoDAO,
            key,
            changes,
            () -> modelVersionNotFound(describeVersion(fullName, version)));
        modelVersionInfo = origModelVersionInfoDAO.toModelVersionInfo();
        modelVersionInfo.setCatalogName(catalogName);
        modelVersionInfo.setSchemaName(schemaName);
        modelVersionInfo.setModelName(registeredModelName);
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
          ErrorCode.INTERNAL, "Error updating model version: " + fullName + "/" + version, e);
    }
    return modelVersionInfo;
  }

  /**
   * Gets the default model storage root from server properties.
   *
   * <p>This is used as a fallback when neither the catalog nor schema has a managed storage
   * location configured. It is being deprecated.
   *
   * @return optional containing the storage root URL if configured in server properties
   */
  private Optional<NormalizedURL> getDefaultModelsStorageRoot() {
    return Optional.ofNullable(serverProperties.get(ServerProperties.Property.MODEL_STORAGE_ROOT))
        .map(NormalizedURL::from);
  }
}
