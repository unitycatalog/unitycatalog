package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateAuthorization;
import io.unitycatalog.server.model.UpdateAuthorization;
import io.unitycatalog.server.model.Authorization;
import io.unitycatalog.server.persist.dao.AuthorizationDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AuthorizationRepository {
    private static final AuthorizationRepository INSTANCE = new AuthorizationRepository();
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationRepository.class);
    private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

    private AuthorizationRepository() {}

    public static AuthorizationRepository getInstance() {
        return INSTANCE;
    }

    public Authorization createAuthorization(CreateAuthorization createAuthorization) {
        Authorization authorization = new Authorization()
                .id(UUID.randomUUID().toString())
                .principalId(createAuthorization.getPrincipalId())
                .principalType(createAuthorization.getPrincipalType())
                .privilege(createAuthorization.getPrivilege())
                .resourceId(createAuthorization.getResourceId())
                .resourceType(createAuthorization.getResourceType())
                .createdAt(System.currentTimeMillis());

        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                session.persist(AuthorizationDAO.from(authorization));
                tx.commit();
                return authorization;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    // TODO: this should perhaps not be based on the ID?
    public Authorization getAuthorization(UUID id) {
        try (Session session = SESSION_FACTORY.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                AuthorizationDAO authorizationDAO = getAuthorizationById(session, id);
                if (authorizationDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Authorization not found: " + id);
                }
                tx.commit();
                return authorizationDAO.toAuthorization();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public AuthorizationDAO getAuthorizationById(Session session, UUID id) {
        Query<AuthorizationDAO> query = session.createQuery("FROM AuthorizationDAO WHERE id = :id", AuthorizationDAO.class);
        query.setParameter("id", id);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public Authorization updateAuthorization(UUID id, UpdateAuthorization updateAuthorization) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                AuthorizationDAO authorizationDAO = getAuthorizationById(session, id);
                if (authorizationDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Authorization not found: " + id);
                }
                if (updateAuthorization.getPrincipalId() != null) {
                    authorizationDAO.setPrincipalId(UUID.fromString(updateAuthorization.getPrincipalId()));
                }
                if (updateAuthorization.getPrincipalType() != null) {
                    authorizationDAO.setPrincipalType(updateAuthorization.getPrincipalType().toString());
                }
                if (updateAuthorization.getPrivilege() != null) {
                    authorizationDAO.setPrivilege(updateAuthorization.getPrivilege().toString());
                }
                if (updateAuthorization.getResourceId() != null) {
                    authorizationDAO.setResourceId(UUID.fromString(updateAuthorization.getResourceId()));
                }
                if (updateAuthorization.getResourceType() != null) {
                    authorizationDAO.setResourceType(updateAuthorization.getResourceType().toString());
                }
                session.merge(authorizationDAO);
                tx.commit();
                return authorizationDAO.toAuthorization();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteAuthorization(UUID id) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                AuthorizationDAO authorizationDAO = getAuthorizationById(session, id);
                if (authorizationDAO != null) {
                    session.remove(authorizationDAO);
                    tx.commit();
                    LOGGER.info("Deleted authorization: {}", id);
                } else {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Authorization not found: " + id);
                }
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
