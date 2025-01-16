package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.UriUtils;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
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
  private final FileOperations fileOperations;
  private static final PagedListingHelper<RegisteredModelInfoDAO> REGISTERED_MODEL_LISTING_HELPER =
      new PagedListingHelper<>(RegisteredModelInfoDAO.class);

  public ModelRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.fileOperations = repositories.getFileOperations();
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
      throw new BaseException(ErrorCode.NOT_FOUND, "Registered model not found: " + name);
    }
    return existingRegisteredModelDao;
  }

  public List<RegisteredModelInfoDAO> getAllRegisteredModelsDao(
      Session session, Optional<String> token, Optional<Integer> maxResults) {
    UUID tokenToUse = new UUID(0, 0);
    if (token.isPresent()) {
      tokenToUse = UUID.fromString(token.get());
    }
    String hql = "FROM RegisteredModelInfoDAO t WHERE t.id > :token ORDER BY t.id ASC";
    Query<RegisteredModelInfoDAO> query = session.createQuery(hql, RegisteredModelInfoDAO.class);
    query.setParameter("token", tokenToUse);
    query.setMaxResults(REGISTERED_MODEL_LISTING_HELPER.getPageSize(maxResults));
    return query.getResultList(); // Returns null if no result is found
  }

  public ModelVersionInfoDAO getModelVersionDao(Session session, UUID modelId, Long version) {
    String hql =
        "FROM ModelVersionInfoDAO t WHERE t.registeredModelId = :registeredModelId AND t.version = :version";
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
        "FROM ModelVersionInfoDAO t WHERE t.registeredModelId = :registeredModelId AND t.version > :token ORDER BY t.version ASC";
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

  /** **************** Registered Model handlers ***************** */
  public RegisteredModelInfo getRegisteredModel(String fullName) {
    LOGGER.info("Getting registered model: {}", fullName);
    RegisteredModelInfo registeredModelInfo = null;
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        String[] parts = RepositoryUtils.parseFullName(fullName);
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
    UUID schemaId = getSchemaId(session, catalogName, schemaName);
    return getRegisteredModelDao(session, schemaId, registeredModelName);
  }

  public RegisteredModelInfo createRegisteredModel(CreateRegisteredModel createRegisteredModel) {
    ValidationUtils.validateSqlObjectName(createRegisteredModel.getName());
    long createTime = System.currentTimeMillis();
    String modelId = UUID.randomUUID().toString();
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    RegisteredModelInfo registeredModelInfo =
        new RegisteredModelInfo()
            .id(modelId)
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
      UUID schemaId = getSchemaId(session, catalogName, schemaName);
      UUID catalogId = getCatalogId(session, catalogName);
      String storageLocation =
          fileOperations.getModelStorageLocation(
              catalogId.toString(), schemaId.toString(), modelId);
      try {
        // Check if registered model already exists
        RegisteredModelInfoDAO existingRegisteredModel =
            getRegisteredModelDao(session, schemaId, registeredModelInfo.getName());
        if (existingRegisteredModel != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS, "Registered model already exists: " + fullName);
        }
        registeredModelInfo.setStorageLocation(storageLocation);
        RegisteredModelInfoDAO registeredModelInfoDAO =
            RegisteredModelInfoDAO.from(registeredModelInfo);
        registeredModelInfoDAO.setSchemaId(schemaId);
        registeredModelInfoDAO.setMaxVersionNumber(0L);
        session.persist(registeredModelInfoDAO);
        UriUtils.createStorageLocationPath(storageLocation);
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
          String nextPageToken =
              REGISTERED_MODEL_LISTING_HELPER.getNextPageToken(
                  registeredModelInfoDAOList, maxResults);
          List<RegisteredModelInfo> result = new ArrayList<>();
          for (RegisteredModelInfoDAO registeredModelInfoDAO : registeredModelInfoDAOList) {
            SchemaInfoDAO schemaInfoDAO =
                RepositoryUtils.getSchemaByIdOrThrow(session, registeredModelInfoDAO.getSchemaId());
            CatalogInfoDAO catalogInfoDAO =
                RepositoryUtils.getCatalogByIdOrThrow(session, schemaInfoDAO.getCatalogId());

            RegisteredModelInfo registeredModelInfo =
                registeredModelInfoDAO.toRegisteredModelInfo();
            registeredModelInfo.setCatalogName(catalogInfoDAO.getName());
            registeredModelInfo.setSchemaName(schemaInfoDAO.getName());
            registeredModelInfo.setFullName(getRegisteredModelFullName(registeredModelInfo));
            result.add(registeredModelInfo);
          }
          return new ListRegisteredModelsResponse()
              .registeredModels(result)
              .nextPageToken(nextPageToken);
        } else {
          LOGGER.info("Listing registered models in {}.{}", catalogName.get(), schemaName.get());
          UUID schemaId = getSchemaId(session, catalogName.get(), schemaName.get());
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
      String[] parts = RepositoryUtils.parseFullName(fullName);
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
          throw new BaseException(ErrorCode.NOT_FOUND, "Registered model not found: " + fullName);
        }
        if (updateRegisteredModel.getNewName() != null) {
          origRegisteredModelInfoDAO.setName(updateRegisteredModel.getNewName());
        }
        if (updateRegisteredModel.getComment() != null) {
          origRegisteredModelInfoDAO.setComment(updateRegisteredModel.getComment());
        }
        long updatedTime = System.currentTimeMillis();
        origRegisteredModelInfoDAO.setUpdatedAt(new Date(updatedTime));
        origRegisteredModelInfoDAO.setUpdatedBy(callerId);
        session.persist(origRegisteredModelInfoDAO);
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
      Transaction tx = session.beginTransaction();
      String[] parts = fullName.split("\\.");
      if (parts.length != 3) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "Invalid registered model name: " + fullName);
      }
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      try {
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
        deleteRegisteredModel(session, schemaId, registeredModelName, force);
        tx.commit();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    }
  }

  public void deleteRegisteredModel(
      Session session, UUID schemaId, String registeredModelName, boolean force) {
    RegisteredModelInfoDAO registeredModelInfoDAO =
        getRegisteredModelDaoOrThrow(session, schemaId, registeredModelName);
    if (force) {
      // Remove all model versions
      List<ModelVersionInfoDAO> versionList =
          getModelVersionsDao(session, registeredModelInfoDAO.getId(), "0", 1);
      while (versionList.size() > 0) {
        for (ModelVersionInfoDAO modelVersionInfoDao : versionList) {
          session.remove(modelVersionInfoDao);
        }
        versionList = getModelVersionsDao(session, registeredModelInfoDAO.getId(), "0", 1);
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
    session.remove(registeredModelInfoDAO);
  }

  /** **************** Model version handlers ***************** */
  public ModelVersionInfo getModelVersion(String fullName, long version) {
    LOGGER.info("Getting model version: {}/{}", fullName, version);
    ModelVersionInfo modelVersionInfo = null;
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        String[] parts = RepositoryUtils.parseFullName(fullName);
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
    String modelVersionId = UUID.randomUUID().toString();
    String catalogName = createModelVersion.getCatalogName();
    String schemaName = createModelVersion.getSchemaName();
    String modelName = createModelVersion.getModelName();
    ModelVersionInfo modelVersionInfo =
        new ModelVersionInfo()
            .id(modelVersionId)
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
      UUID catalogId = getCatalogId(session, catalogName);
      UUID schemaId = getSchemaId(session, catalogName, schemaName);
      String storageLocation = "";
      try {
        // Check if registered model already exists
        RegisteredModelInfoDAO existingRegisteredModel =
            getRegisteredModelDaoOrThrow(session, schemaId, modelName);
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
            fileOperations.getModelVersionStorageLocation(
                catalogId.toString(), schemaId.toString(), modelId.toString(), modelVersionId);
        modelVersionInfo.setVersion(version);
        modelVersionInfo.setStorageLocation(storageLocation);
        ModelVersionInfoDAO modelVersionInfoDAO = ModelVersionInfoDAO.from(modelVersionInfo);
        modelVersionInfoDAO.setRegisteredModelId(modelId);
        session.persist(modelVersionInfoDAO);
        UriUtils.createStorageLocationPath(storageLocation);
        // update the registered model
        existingRegisteredModel.setMaxVersionNumber(version);
        session.persist(existingRegisteredModel);
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
        String[] parts = registeredModelFullName.split("\\.");
        if (parts.length != 3) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Invalid registered model name: " + registeredModelFullName);
        }
        String catalogName = parts[0];
        String schemaName = parts[1];
        String registeredModelName = parts[2];
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
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
      String[] parts = RepositoryUtils.parseFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      tx = session.beginTransaction();
      try {
        // Get the registered model record from the database
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
        // Get the model version record from the database
        ModelVersionInfoDAO origModelVersionInfoDAO =
            getModelVersionDaoOrThrow(session, schemaId, fullName, registeredModelName, version);
        origModelVersionInfoDAO.setComment(updateModelVersion.getComment());
        long updatedTime = System.currentTimeMillis();
        origModelVersionInfoDAO.setUpdatedAt(new Date(updatedTime));
        origModelVersionInfoDAO.setUpdatedBy(callerId);
        session.persist(origModelVersionInfoDAO);
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
    String[] parts = fullName.split("\\.");
    if (parts.length != 3) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Invalid registered model name: " + fullName);
    }
    String catalogName = parts[0];
    String schemaName = parts[1];
    String registeredModelName = parts[2];
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
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
    ModelVersionInfoDAO modelVersionInfoDAO =
        getModelVersionDao(session, registeredModelId, version);
    if (modelVersionInfoDAO == null) {
      throw new BaseException(
          ErrorCode.NOT_FOUND, "Model version not found: " + fullName + "/" + version);
    }
    session.remove(modelVersionInfoDAO);
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
      String[] parts = RepositoryUtils.parseFullName(fullName);
      String catalogName = parts[0];
      String schemaName = parts[1];
      String registeredModelName = parts[2];
      tx = session.beginTransaction();
      try {
        // Get the registered model record from the database
        UUID schemaId = getSchemaId(session, catalogName, schemaName);
        ModelVersionInfoDAO origModelVersionInfoDAO =
            getModelVersionDaoOrThrow(session, schemaId, fullName, registeredModelName, version);

        if (ModelVersionStatus.valueOf(origModelVersionInfoDAO.getStatus())
            != ModelVersionStatus.PENDING_REGISTRATION) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Model version not in a pending registration state: " + fullName + "/" + version);
        }
        origModelVersionInfoDAO.setStatus(ModelVersionStatus.READY.toString());
        long updatedTime = System.currentTimeMillis();
        origModelVersionInfoDAO.setUpdatedAt(new Date(updatedTime));
        origModelVersionInfoDAO.setUpdatedBy(callerId);
        session.persist(origModelVersionInfoDAO);
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

  public UUID getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo =
        repositories.getSchemaRepository().getSchemaDAO(session, catalogName, schemaName);
    if (schemaInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
    return schemaInfo.getId();
  }

  public UUID getCatalogId(Session session, String catalogName) {
    CatalogInfoDAO catalogInfo =
        repositories.getCatalogRepository().getCatalogDAO(session, catalogName);
    if (catalogInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
    }
    return catalogInfo.getId();
  }
}
