package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogRepository {
  private static final CatalogRepository INSTANCE = new CatalogRepository();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final PagedListingHelper<CatalogInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(CatalogInfoDAO.class);

  private CatalogRepository() {}

  public static CatalogRepository getInstance() {
    return INSTANCE;
  }

  public CatalogInfo addCatalog(CreateCatalog createCatalog) {
    ValidationUtils.validateSqlObjectName(createCatalog.getName());
    CatalogInfo catalogInfo =
        new CatalogInfo()
            .id(java.util.UUID.randomUUID().toString())
            .comment(createCatalog.getComment())
            .name(createCatalog.getName())
            .createdAt(System.currentTimeMillis())
            .properties(createCatalog.getProperties());

    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        if (getCatalogDAO(session, createCatalog.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS, "Catalog already exists: " + createCatalog.getName());
        }
        CatalogInfoDAO catalogInfoDAO = CatalogInfoDAO.from(catalogInfo);
        PropertyDAO.from(catalogInfo.getProperties(), catalogInfoDAO.getId(), Constants.CATALOG)
            .forEach(session::persist);
        session.persist(catalogInfoDAO);
        tx.commit();
        LOGGER.info("Added catalog: {}", catalogInfo.getName());
        return catalogInfo;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
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
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        ListCatalogsResponse response = listCatalogs(session, maxResults, pageToken);
        tx.commit();
        return response;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
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
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        CatalogInfoDAO catalogInfoDAO = getCatalogDAO(session, name);
        if (catalogInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
        }
        tx.commit();
        CatalogInfo catalogInfo = catalogInfoDAO.toCatalogInfo();
        return RepositoryUtils.attachProperties(
            catalogInfo, catalogInfo.getId(), Constants.CATALOG, session);
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public CatalogInfoDAO getCatalogDAO(Session session, String name) {
    Query<CatalogInfoDAO> query =
        session.createQuery("FROM CatalogInfoDAO WHERE name = :value", CatalogInfoDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) {
    if (updateCatalog.getNewName() != null) {
      ValidationUtils.validateSqlObjectName(updateCatalog.getNewName());
    }
    // cna make this just update once we have an identifier that is not the name
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        CatalogInfoDAO catalogInfoDAO = getCatalogDAO(session, name);
        if (catalogInfoDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
        }
        if (updateCatalog.getNewName() == null
            && updateCatalog.getComment() == null
            && (updateCatalog.getProperties() == null || updateCatalog.getProperties().isEmpty())) {
          tx.rollback();
          return catalogInfoDAO.toCatalogInfo();
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
        if (!updateCatalog.getProperties().isEmpty()) {
          PropertyRepository.findProperties(session, catalogInfoDAO.getId(), Constants.CATALOG)
              .forEach(session::remove);
          session.flush();
          PropertyDAO.from(updateCatalog.getProperties(), catalogInfoDAO.getId(), Constants.CATALOG)
              .forEach(session::persist);
        }
        catalogInfoDAO.setUpdatedAt(new Date());
        session.merge(catalogInfoDAO);
        tx.commit();
        return catalogInfoDAO.toCatalogInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public void deleteCatalog(String name, boolean force) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        CatalogInfoDAO catalogInfo = getCatalogDAO(session, name);
        if (catalogInfo != null) {
          // Check if there are any schemas in the catalog
          List<SchemaInfo> schemas =
              SCHEMA_REPOSITORY
                  .listSchemas(
                      session,
                      catalogInfo.getId(),
                      catalogInfo.getName(),
                      Optional.of(1),
                      Optional.empty())
                  .getSchemas();
          if (schemas != null && !schemas.isEmpty()) {
            if (!force) {
              throw new BaseException(
                  ErrorCode.FAILED_PRECONDITION, "Cannot delete catalog with schemas: " + name);
            }
            String nextToken = null;
            do {
              ListSchemasResponse listSchemasResponse =
                  SCHEMA_REPOSITORY.listSchemas(
                      session,
                      catalogInfo.getId(),
                      catalogInfo.getName(),
                      Optional.empty(),
                      Optional.ofNullable(nextToken));
              for (SchemaInfo schemaInfo : listSchemasResponse.getSchemas()) {
                SCHEMA_REPOSITORY.deleteSchema(
                    session,
                    catalogInfo.getId(),
                    catalogInfo.getName(),
                    schemaInfo.getName(),
                    true);
              }
              nextToken = listSchemasResponse.getNextPageToken();
            } while (nextToken != null);
          }
          PropertyRepository.findProperties(session, catalogInfo.getId(), Constants.CATALOG)
              .forEach(session::remove);
          session.remove(catalogInfo);
          tx.commit();
          LOGGER.info("Deleted catalog: {}", catalogInfo.getName());
        } else {
          throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
        }
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
