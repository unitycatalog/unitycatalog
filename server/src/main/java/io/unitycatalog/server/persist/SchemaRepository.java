package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class SchemaRepository {
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<SchemaInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(SchemaInfoDAO.class);

  public SchemaRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public SchemaInfo createSchema(CreateSchema createSchema) {
    ValidationUtils.validateSqlObjectName(createSchema.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        if (getSchemaDAO(session, createSchema.getCatalogName(), createSchema.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS, "Schema already exists: " + createSchema.getName());
        }
        CatalogInfoDAO catalogDAO =
            repositories
                .getCatalogRepository()
                .getCatalogDAO(session, createSchema.getCatalogName());
        Long createTime = System.currentTimeMillis();
        SchemaInfo schemaInfo =
            new SchemaInfo()
                .schemaId(UUID.randomUUID().toString())
                .name(createSchema.getName())
                .catalogName(createSchema.getCatalogName())
                .comment(createSchema.getComment())
                .owner(callerId)
                .createdAt(createTime)
                .createdBy(callerId)
                .updatedAt(createTime)
                .updatedBy(callerId)
                .properties(createSchema.getProperties());
        SchemaInfoDAO schemaInfoDAO = SchemaInfoDAO.from(schemaInfo);
        schemaInfoDAO.setCatalogId(catalogDAO.getId());
        PropertyDAO.from(schemaInfo.getProperties(), schemaInfoDAO.getId(), Constants.SCHEMA)
            .forEach(session::persist);
        session.persist(schemaInfoDAO);
        tx.commit();
        addNamespaceData(schemaInfo, createSchema.getCatalogName());
        return schemaInfo;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  private void addNamespaceData(SchemaInfo schemaInfo, String catalogName) {
    schemaInfo.setCatalogName(catalogName);
    schemaInfo.setFullName(catalogName + "." + schemaInfo.getName());
  }

  private SchemaInfo convertFromDAO(Session session, SchemaInfoDAO schemaInfoDAO, String fullName) {
    String catalogName = fullName.split("\\.")[0];
    SchemaInfo schemaInfo = schemaInfoDAO.toSchemaInfo();
    addNamespaceData(schemaInfo, catalogName);
    return RepositoryUtils.attachProperties(
        schemaInfo, schemaInfo.getSchemaId(), Constants.SCHEMA, session);
  }

  public SchemaInfoDAO getSchemaDAO(Session session, UUID catalogId, String schemaName) {
    Query<SchemaInfoDAO> query =
        session.createQuery(
            "FROM SchemaInfoDAO WHERE name = :name and catalogId = :catalogId",
            SchemaInfoDAO.class);
    query.setParameter("name", schemaName);
    query.setParameter("catalogId", catalogId);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public SchemaInfoDAO getSchemaDAO(Session session, String catalogName, String schemaName) {
    CatalogInfoDAO catalog =
        repositories.getCatalogRepository().getCatalogDAO(session, catalogName);
    if (catalog == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
    }
    return getSchemaDAO(session, catalog.getId(), schemaName);
  }

  public SchemaInfoDAO getSchemaDAO(Session session, String fullName) {
    String[] namespace = fullName.split("\\.");
    return getSchemaDAO(session, namespace[0], namespace[1]);
  }

  public UUID getCatalogId(Session session, String catalogName) {
    CatalogInfoDAO catalogInfo =
        repositories.getCatalogRepository().getCatalogDAO(session, catalogName);
    if (catalogInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
    }
    return catalogInfo.getId();
  }

  /**
   * Return the list of schemas in ascending order of schema name.
   *
   * @param catalogName
   * @param maxResults
   * @param pageToken
   * @return
   */
  public ListSchemasResponse listSchemas(
      String catalogName, Optional<Integer> maxResults, Optional<String> pageToken) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      // TODO: Implement pagination and filtering if required
      // For now, returning all schemas without pagination
      try {
        UUID catalogId = getCatalogId(session, catalogName);
        ListSchemasResponse response =
            listSchemas(session, catalogId, catalogName, maxResults, pageToken);
        tx.commit();
        return response;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public ListSchemasResponse listSchemas(
      Session session,
      UUID catalogId,
      String catalogName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    List<SchemaInfoDAO> schemaInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, catalogId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(schemaInfoDAOList, maxResults);
    List<SchemaInfo> result = new ArrayList<>();
    for (SchemaInfoDAO schemaInfoDAO : schemaInfoDAOList) {
      SchemaInfo schemaInfo = schemaInfoDAO.toSchemaInfo();
      RepositoryUtils.attachProperties(
          schemaInfo, schemaInfo.getSchemaId(), Constants.SCHEMA, session);
      addNamespaceData(schemaInfo, catalogName);
      result.add(schemaInfo);
    }
    return new ListSchemasResponse().schemas(result).nextPageToken(nextPageToken);
  }

  public SchemaInfo getSchema(String fullName) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      SchemaInfoDAO schemaInfoDAO;
      try {
        schemaInfoDAO = getSchemaDAO(session, fullName);
        if (schemaInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
        }
        tx.commit();
        SchemaInfo schemaInfo = convertFromDAO(session, schemaInfoDAO, fullName);
        return RepositoryUtils.attachProperties(
            schemaInfo, schemaInfo.getSchemaId(), Constants.SCHEMA, session);
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public SchemaInfo updateSchema(String fullName, UpdateSchema updateSchema) {
    if (updateSchema.getNewName() != null) {
      ValidationUtils.validateSqlObjectName(updateSchema.getNewName());
    }
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        SchemaInfoDAO schemaInfoDAO = getSchemaDAO(session, fullName);
        if (schemaInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
        }
        if (updateSchema.getNewName() != null) {
          if (getSchemaDAO(session, fullName.split("\\.")[0], updateSchema.getNewName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "Schema already exists: " + updateSchema.getNewName());
          }
        }
        if (updateSchema.getComment() == null
            && updateSchema.getNewName() == null
            && (updateSchema.getProperties() == null || updateSchema.getProperties().isEmpty())) {
          tx.rollback();
          return convertFromDAO(session, schemaInfoDAO, fullName);
        }
        // Update the schema with new values
        if (updateSchema.getComment() != null) {
          schemaInfoDAO.setComment(updateSchema.getComment());
        }
        if (updateSchema.getNewName() != null) {
          schemaInfoDAO.setName(updateSchema.getNewName());
        }
        if (updateSchema.getProperties() != null && !updateSchema.getProperties().isEmpty()) {
          PropertyRepository.findProperties(session, schemaInfoDAO.getId(), Constants.SCHEMA)
              .forEach(session::remove);
          session.flush();
          PropertyDAO.from(updateSchema.getProperties(), schemaInfoDAO.getId(), Constants.SCHEMA)
              .forEach(session::persist);
        }
        schemaInfoDAO.setUpdatedAt(new Date());
        schemaInfoDAO.setUpdatedBy(callerId);
        session.merge(schemaInfoDAO);
        tx.commit();
        return convertFromDAO(session, schemaInfoDAO, fullName);
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public void deleteSchema(String fullName, boolean force) {
    try (Session session = sessionFactory.openSession()) {
      String[] namespace = fullName.split("\\.");
      if (namespace.length != 2) {
        throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid schema name: " + fullName);
      }
      CatalogInfoDAO catalog =
          repositories.getCatalogRepository().getCatalogDAO(session, namespace[0]);
      if (catalog == null) {
        throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + namespace[0]);
      }
      Transaction tx = session.beginTransaction();
      try {
        deleteSchema(session, catalog.getId(), namespace[0], namespace[1], force);
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public void processChildTables(
      Session session, UUID schemaId, String catalogName, String schemaName, boolean force) {
    // first check if there are any child tables
    List<TableInfo> tables =
        repositories
            .getTableRepository()
            .listTables(
                session,
                schemaId,
                catalogName,
                schemaName,
                Optional.of(1),
                Optional.empty(),
                true,
                true)
            .getTables();
    if (tables != null && !tables.isEmpty()) {
      if (!force) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Cannot delete schema with tables");
      }
      String nextToken = null;
      do {
        ListTablesResponse listTablesResponse =
            repositories
                .getTableRepository()
                .listTables(
                    session,
                    schemaId,
                    catalogName,
                    schemaName,
                    Optional.empty(),
                    Optional.ofNullable(nextToken),
                    true,
                    true);
        for (TableInfo tableInfo : listTablesResponse.getTables()) {
          repositories.getTableRepository().deleteTable(session, schemaId, tableInfo.getName());
        }
        nextToken = listTablesResponse.getNextPageToken();
      } while (nextToken != null);
    }
  }

  public void processChildVolumes(
      Session session, UUID schemaId, String catalogName, String schemaName, boolean force) {
    // first check if there are any child volumes
    List<VolumeInfo> volumes =
        repositories
            .getVolumeRepository()
            .listVolumes(
                session, schemaId, catalogName, schemaName, Optional.of(1), Optional.empty())
            .getVolumes();
    if (volumes != null && !volumes.isEmpty()) {
      if (!force) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Cannot delete schema with volumes");
      }
      String nextToken = null;
      do {
        ListVolumesResponseContent listVolumesResponse =
            repositories
                .getVolumeRepository()
                .listVolumes(
                    session,
                    schemaId,
                    catalogName,
                    schemaName,
                    Optional.empty(),
                    Optional.ofNullable(nextToken));
        for (VolumeInfo volumeInfo : listVolumesResponse.getVolumes()) {
          repositories.getVolumeRepository().deleteVolume(session, schemaId, volumeInfo.getName());
        }
        nextToken = listVolumesResponse.getNextPageToken();
      } while (nextToken != null);
    }
  }

  public void processChildFunctions(
      Session session, UUID schemaId, String catalogName, String schemaName, boolean force) {
    // first check if there are any child functions
    List<FunctionInfo> functions =
        repositories
            .getFunctionRepository()
            .listFunctions(
                session, schemaId, catalogName, schemaName, Optional.of(1), Optional.empty())
            .getFunctions();
    if (functions != null && !functions.isEmpty()) {
      if (!force) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION, "Cannot delete schema with functions");
      }
      String nextToken = null;
      do {
        ListFunctionsResponse listFunctionsResponse =
            repositories
                .getFunctionRepository()
                .listFunctions(
                    session,
                    schemaId,
                    catalogName,
                    schemaName,
                    Optional.empty(),
                    Optional.ofNullable(nextToken));
        for (FunctionInfo functionInfo : listFunctionsResponse.getFunctions()) {
          repositories
              .getFunctionRepository()
              .deleteFunction(session, schemaId, functionInfo.getName());
        }
        nextToken = listFunctionsResponse.getNextPageToken();
      } while (nextToken != null);
    }
  }

  public void processChildModels(
      Session session, UUID schemaId, String catalogName, String schemaName, boolean force) {
    // first check if there are any child Models
    List<RegisteredModelInfo> registeredModels =
        repositories
            .getModelRepository()
            .listRegisteredModels(
                session, schemaId, catalogName, schemaName, Optional.of(1), Optional.empty())
            .getRegisteredModels();
    if (registeredModels != null && !registeredModels.isEmpty()) {
      if (!force) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Cannot delete schema with models");
      }
      String nextToken = null;
      do {
        ListRegisteredModelsResponse listRegisteredModelsResponse =
            repositories
                .getModelRepository()
                .listRegisteredModels(
                    session,
                    schemaId,
                    catalogName,
                    schemaName,
                    Optional.empty(),
                    Optional.ofNullable(nextToken));
        for (RegisteredModelInfo registeredModelInfo :
            listRegisteredModelsResponse.getRegisteredModels()) {
          repositories
              .getModelRepository()
              .deleteRegisteredModel(session, schemaId, registeredModelInfo.getName(), true);
        }
        nextToken = listRegisteredModelsResponse.getNextPageToken();
      } while (nextToken != null);
    }
  }

  public void deleteSchema(
      Session session, UUID catalogId, String catalogName, String schemaName, boolean force) {
    SchemaInfoDAO schemaInfo = getSchemaDAO(session, catalogId, schemaName);
    if (schemaInfo != null) {
      processChildTables(session, schemaInfo.getId(), catalogName, schemaName, force);
      processChildVolumes(session, schemaInfo.getId(), catalogName, schemaName, force);
      processChildFunctions(session, schemaInfo.getId(), catalogName, schemaName, force);
      processChildModels(session, schemaInfo.getId(), catalogName, schemaName, force);
      session.remove(schemaInfo);
      PropertyRepository.findProperties(session, schemaInfo.getId(), Constants.SCHEMA)
          .forEach(session::remove);
    } else {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
  }
}
