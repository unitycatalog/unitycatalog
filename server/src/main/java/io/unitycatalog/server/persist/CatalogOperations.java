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

public class CatalogOperations {
    @Getter
    private static final CatalogOperations instance = new CatalogOperations();
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogOperations.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();
    private CatalogOperations() {}

    public CatalogInfo addCatalog(CreateCatalog createCatalog) {
        ValidationUtils.validateSqlObjectName(createCatalog.getName());
        CatalogInfoDAO catalogInfo = new CatalogInfoDAO();
        catalogInfo.setId(java.util.UUID.randomUUID());
        catalogInfo.setName(createCatalog.getName());
        catalogInfo.setComment(createCatalog.getComment());
        catalogInfo.setCreatedAt(new Date());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            session.persist(catalogInfo);
            tx.commit();
            System.out.println("Added catalog: " + catalogInfo.getName());
            return CatalogInfoDAO.toCatalogInfo(catalogInfo);
        } catch(Exception e) {
            LOGGER.error("Error adding catalog", e);
            return null;
        }
    }

    public ListCatalogsResponse listCatalogs() {
        ListCatalogsResponse response = new ListCatalogsResponse();
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            response.setCatalogs(session
                    .createQuery("from CatalogInfoDAO ", CatalogInfoDAO.class).list()
                    .stream().map(CatalogInfoDAO::toCatalogInfo).collect(Collectors.toList()));
            return response;
        } catch(Exception e) {
            LOGGER.error("Error listing catalogs", e);
            return null;
        }
    }

    public CatalogInfo getCatalog(String name) {
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            return getCatalog(session, name);
        } catch(Exception e) {
            LOGGER.error("Error getting catalog", e);
            return null;
        }
    }

    public CatalogInfo getCatalog(Session session, String name) {
        CatalogInfoDAO catalogInfo = getCatalogInfoDAO(session, name);
        if (catalogInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
        }
        return CatalogInfoDAO.toCatalogInfo(catalogInfo);
    }

    public CatalogInfoDAO getCatalogInfoDAO(Session session, String name) {
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
            CatalogInfoDAO catalogInfo = getCatalogInfoDAO(session, name);
            catalogInfo.setName(updateCatalog.getNewName());
            catalogInfo.setComment(updateCatalog.getComment());
            catalogInfo.setUpdatedAt(new Date());
            session.merge(catalogInfo);
            tx.commit();
            return CatalogInfoDAO.toCatalogInfo(catalogInfo);
        } catch(Exception e) {
            LOGGER.error("Error updating catalog", e);
            return null;
        }
    }

    public void deleteCatalog(String name) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            CatalogInfoDAO catalogInfo = getCatalogInfoDAO(session, name);
            if (catalogInfo != null) {
                session.remove(catalogInfo);
                tx.commit();
                LOGGER.info("Deleted catalog: {}", catalogInfo.getName());
            } else {
                throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + name);
            }
        } catch (Exception e) {
            LOGGER.error("Error deleting catalog", e);
        }
    }
}
