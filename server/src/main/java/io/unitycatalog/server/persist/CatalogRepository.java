package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CatalogRepository {
    @Getter
    private static final CatalogRepository instance = new CatalogRepository();
    private static final SchemaRepository schemaRepository = SchemaRepository.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRepository.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();
    private CatalogRepository() {}

    public CatalogInfo addCatalog(CreateCatalog createCatalog) {
        ValidationUtils.validateSqlObjectName(createCatalog.getName());
        CatalogInfoDAO catalogInfo = new CatalogInfoDAO();
        catalogInfo.setId(java.util.UUID.randomUUID());
        catalogInfo.setName(createCatalog.getName());
        catalogInfo.setComment(createCatalog.getComment());
        catalogInfo.setCreatedAt(new Date());

        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                if (getCatalogDAO(session, createCatalog.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Catalog already exists: " + createCatalog.getName());
                }
                session.persist(catalogInfo);
                tx.commit();
                System.out.println("Added catalog: " + catalogInfo.getName());
                return CatalogInfoDAO.toCatalogInfo(catalogInfo);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public ListCatalogsResponse listCatalogs() {
        ListCatalogsResponse response = new ListCatalogsResponse();
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                response.setCatalogs(session
                        .createQuery("from CatalogInfoDAO ", CatalogInfoDAO.class).list()
                        .stream().map(CatalogInfoDAO::toCatalogInfo).collect(Collectors.toList()));
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
            return response;
        }
    }

    public CatalogInfo getCatalog(String name) {
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            CatalogInfoDAO catalogInfo = null;
            try {
                catalogInfo = getCatalogDAO(session, name);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
            if (catalogInfo == null) {
                throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
            }
            return CatalogInfoDAO.
                    toCatalogInfo(getCatalogDAO(session, name));
        }
    }

    public CatalogInfoDAO getCatalogDAO(Session session, String name) {
        Query<CatalogInfoDAO> query = session
                .createQuery("FROM CatalogInfoDAO WHERE name = :value", CatalogInfoDAO.class);
        query.setParameter("value", name);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public CatalogInfo updateCatalog(String name, UpdateCatalog updateCatalog) {
        ValidationUtils.validateSqlObjectName(updateCatalog.getNewName());
        // cna make this just update once we have an identifier that is not the name
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                CatalogInfoDAO catalogInfo = getCatalogDAO(session, name);
                if (catalogInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
                }
                if (getCatalogDAO(session, updateCatalog.getNewName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Catalog already exists: " + updateCatalog.getNewName());
                }
                catalogInfo.setName(updateCatalog.getNewName());
                catalogInfo.setComment(updateCatalog.getComment());
                catalogInfo.setUpdatedAt(new Date());
                session.merge(catalogInfo);
                tx.commit();
                return CatalogInfoDAO.toCatalogInfo(catalogInfo);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteCatalog(String name, boolean force) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                CatalogInfoDAO catalogInfo = getCatalogDAO(session, name);
                if (catalogInfo != null) {
                    // Check if there are any schemas in the catalog
                    List<SchemaInfoDAO> schemas = schemaRepository.listSchemas(session, catalogInfo.getId(), Optional.of(1));
                    if (schemas != null && !schemas.isEmpty()) {
                        if (!force) {
                            throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                                    "Cannot delete catalog with schemas: " + name);
                        }
                        List<SchemaInfoDAO> allChildSchemas = schemaRepository.listSchemas(session, catalogInfo.getId(), Optional.empty());
                        for (SchemaInfoDAO schemaInfo : allChildSchemas) {
                            schemaRepository.deleteSchema(session, catalogInfo.getId(), schemaInfo.getName(), true);
                        }
                    }
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
