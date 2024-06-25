package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.query.Query;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.UpdateCatalog;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.stream.Collectors;

public class CatalogRepository {
    @Getter
    private static final CatalogRepository instance = new CatalogRepository();
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRepository.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();
    private CatalogRepository() {}

    public CatalogInfo addCatalog(CreateCatalog createCatalog) {
        ValidationUtils.validateSqlObjectName(createCatalog.getName());
        CatalogInfo catalogInfo = new CatalogInfo()
                .id(java.util.UUID.randomUUID().toString())
                .comment(createCatalog.getComment())
                .name(createCatalog.getName())
                .createdAt(System.currentTimeMillis())
                .properties(createCatalog.getProperties());

        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                if (getCatalogDAO(session, createCatalog.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Catalog already exists: " + createCatalog.getName());
                }
                CatalogInfoDAO catalogInfoDAO = CatalogInfoDAO.from(catalogInfo);
                catalogInfoDAO.getProperties().forEach(p -> p.setCatalog(catalogInfoDAO));
                session.persist(catalogInfoDAO);
                tx.commit();
                return catalogInfo;
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
                        .createQuery("from CatalogInfoDAO", CatalogInfoDAO.class).list()
                        .stream().map(c -> c.toCatalogInfo()).collect(Collectors.toList()));
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
            CatalogInfoDAO catalogInfoDAO;
            try {
                catalogInfoDAO = getCatalogDAO(session, name);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
            if (catalogInfoDAO == null) {
                throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
            }
            return catalogInfoDAO.toCatalogInfo();
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
                CatalogInfoDAO catalogInfoDAO = getCatalogDAO(session, name);
                if (catalogInfoDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
                }
                if (getCatalogDAO(session, updateCatalog.getNewName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Catalog already exists: " + updateCatalog.getNewName());
                }
                catalogInfoDAO.setName(updateCatalog.getNewName());
                catalogInfoDAO.setComment(updateCatalog.getComment());
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

    public void deleteCatalog(String name) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                CatalogInfoDAO catalogInfo = getCatalogDAO(session, name);
                if (catalogInfo != null) {
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
