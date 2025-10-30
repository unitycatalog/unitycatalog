package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<CatalogInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(CatalogInfoDAO.class);

  public CatalogRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public CatalogInfo addCatalog(CreateCatalog createCatalog) {
    ValidationUtils.validateSqlObjectName(createCatalog.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    Long createTime = System.currentTimeMillis();
    CatalogInfo catalogInfo =
        new CatalogInfo()
            .id(java.util.UUID.randomUUID().toString())
            .comment(createCatalog.getComment())
            .name(createCatalog.getName())
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId)
            .properties(createCatalog.getProperties());

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getCatalogDAO(session, createCatalog.getName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "Catalog already exists: " + createCatalog.getName());
          }
          CatalogInfoDAO catalogInfoDAO = CatalogInfoDAO.from(catalogInfo);
          PropertyDAO.from(catalogInfo.getProperties(), catalogInfoDAO.getId(), Constants.CATALOG)
              .forEach(session::persist);
          session.persist(catalogInfoDAO);
          LOGGER.info("Added catalog: {}", catalogInfo.getName());
          return catalogInfo;
        },
        "Failed to add catalog",
        /* readOnly = */ false);
  }

  /**
   * Return the list of catalogs in ascending order of catalog name.
   *
   * @param maxResults
   * @param pageToken
   * @return
   */
  public ListCatalogsResponse listCatalogs(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> listCatalogs(session, maxResults, pageToken),
        "Failed to list catalogs",
        /* readOnly = */ true);
  }

  public ListCatalogsResponse listCatalogs(
      Session session, Optional<Integer> maxResults, Optional<String> pageToken) {
    List<CatalogInfoDAO> catalogInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, null);
    String nextPageToken = LISTING_HELPER.getNextPageToken(catalogInfoDAOList, maxResults);
    List<CatalogInfo> result = new ArrayList<>();
    for (CatalogInfoDAO catalogInfoDAO : catalogInfoDAOList) {
      CatalogInfo catalogInfo = catalogInfoDAO.toCatalogInfo();
      RepositoryUtils.attachProperties(
          catalogInfo, catalogInfo.getId(), Constants.CATALOG, session);
      result.add(catalogInfo);
    }
    return new ListCatalogsResponse().catalogs(result).nextPageToken(nextPageToken);
  }

  public CatalogInfo getCatalog(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CatalogInfoDAO catalogInfoDAO = getCatalogDAO(session, name);
          if (catalogInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
          }
          CatalogInfo catalogInfo = catalogInfoDAO.toCatalogInfo();
          return RepositoryUtils.attachProperties(
              catalogInfo, catalogInfo.getId(), Constants.CATALOG, session);
        },
        "Failed to get catalog",
        /* readOnly = */ true);
  }

  public CatalogInfoDAO getCatalogDAO(Session session, String name) {
    Query<CatalogInfoDAO> query =
        session.createQuery("FROM CatalogInfoDAO WHERE name = :value", CatalogInfoDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public UUID getCatalogId(Session session, String catalogName) {
    CatalogInfoDAO catalogInfo = getCatalogDAO(session, catalogName);
    if (catalogInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
    }
    return catalogInfo.getId();
  }

  public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) {
    if (updateCatalog.getNewName() != null) {
      ValidationUtils.validateSqlObjectName(updateCatalog.getNewName());
    }
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CatalogInfoDAO catalogInfoDAO = getCatalogDAO(session, name);
          if (catalogInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
          }
          if (updateCatalog.getNewName() == null
              && updateCatalog.getComment() == null
              && (updateCatalog.getProperties() == null
                  || updateCatalog.getProperties().isEmpty())) {
            CatalogInfo catalogInfo = catalogInfoDAO.toCatalogInfo();
            return RepositoryUtils.attachProperties(
                catalogInfo, catalogInfo.getId(), Constants.CATALOG, session);
          }
          if (updateCatalog.getNewName() != null
              && getCatalogDAO(session, updateCatalog.getNewName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "Catalog already exists: " + updateCatalog.getNewName());
          }
          if (updateCatalog.getNewName() != null) {
            catalogInfoDAO.setName(updateCatalog.getNewName());
          }
          if (updateCatalog.getComment() != null) {
            catalogInfoDAO.setComment(updateCatalog.getComment());
          }
          if (updateCatalog.getProperties() != null && !updateCatalog.getProperties().isEmpty()) {
            PropertyRepository.findProperties(session, catalogInfoDAO.getId(), Constants.CATALOG)
                .forEach(session::remove);
            session.flush();
            PropertyDAO.from(
                    updateCatalog.getProperties(), catalogInfoDAO.getId(), Constants.CATALOG)
                .forEach(session::persist);
          }
          catalogInfoDAO.setUpdatedAt(new Date());
          catalogInfoDAO.setUpdatedBy(callerId);
          session.merge(catalogInfoDAO);
          CatalogInfo catalogInfo = catalogInfoDAO.toCatalogInfo();
          return RepositoryUtils.attachProperties(
              catalogInfo, catalogInfo.getId(), Constants.CATALOG, session);
        },
        "Failed to update catalog",
        /* readOnly = */ false);
  }

  public void deleteCatalog(String name, boolean force) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          CatalogInfoDAO catalogInfo = getCatalogDAO(session, name);
          if (catalogInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
          }

          // First, check if there are any schemas in the catalog (to determine if force is needed)
          ListSchemasResponse initialSchemaCheck =
              repositories
                  .getSchemaRepository()
                  .listSchemas(
                      session,
                      catalogInfo.getId(),
                      catalogInfo.getName(),
                      Optional.of(1), // We just need to know if there are any schemas
                      Optional.empty());

          if (!initialSchemaCheck.getSchemas().isEmpty() && !force) {
            throw new BaseException(
                ErrorCode.FAILED_PRECONDITION,
                "Cannot delete catalog with schemas. Use force=true to force deletion.");
          }

          // If we're proceeding with force=true, delete all schemas with pagination
          if (force) {
            String nextPageToken = null;
            do {
              ListSchemasResponse schemaResponse =
                  repositories
                      .getSchemaRepository()
                      .listSchemas(
                          session,
                          catalogInfo.getId(),
                          catalogInfo.getName(),
                          Optional.empty(), // Use default page size for efficiency
                          Optional.ofNullable(nextPageToken));

              // Process this page of schemas
              for (SchemaInfo schema : schemaResponse.getSchemas()) {
                repositories
                    .getSchemaRepository()
                    .deleteSchema(
                        session,
                        catalogInfo.getId(),
                        catalogInfo.getName(),
                        schema.getName(),
                        force);
              }

              // Get the token for the next page
              nextPageToken = schemaResponse.getNextPageToken();
            } while (nextPageToken != null);
          }

          // Delete properties
          PropertyRepository.findProperties(session, catalogInfo.getId(), Constants.CATALOG)
              .forEach(session::remove);

          // Remove the catalog
          session.remove(catalogInfo);
          LOGGER.info("Deleted catalog: {}", name);
          return null;
        },
        "Failed to delete catalog",
        /* readOnly = */ false);
  }
}
