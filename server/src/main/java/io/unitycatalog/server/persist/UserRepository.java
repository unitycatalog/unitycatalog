package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

public class UserRepository {
    private static final UserRepository INSTANCE = new UserRepository();
    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
    private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

    private UserRepository() {}

    public static UserRepository getInstance() {
        return INSTANCE;
    }

    public User createUser(User ) {
        User userDAO = UserDAO.builder()
                .email(email)
                .externalId(externalId)
                .state(state)
                .createdAt(new Date())
                .updatedAt(new Date())
                .build();

        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                if (getUserByEmail(session, email) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS, "User already exists: " + email);
                }
                session.persist(userDAO);
                tx.commit();
                LOGGER.info("Added user: {}", email);
                return userDAO;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public UserDAO getUserByEmail(String email) {
        try (Session session = SESSION_FACTORY.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                UserDAO userDAO = getUserByEmail(session, email);
                if (userDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + email);
                }
                tx.commit();
                return userDAO;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public UserDAO getUserByEmail(Session session, String email) {
        Query<UserDAO> query = session.createQuery("FROM UserDAO WHERE email = :email", UserDAO.class);
        query.setParameter("email", email);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public UserDAO updateUser(String email, Optional<String> newEmail, Optional<String> externalId, Optional<String> state) {
        newEmail.ifPresent(ValidationUtils::validateEmail);

        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                UserDAO userDAO = getUserByEmail(session, email);
                if (userDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + email);
                }
                newEmail.ifPresent(userDAO::setEmail);
                externalId.ifPresent(userDAO::setExternalId);
                state.ifPresent(userDAO::setState);
                userDAO.setUpdatedAt(new Date());
                session.merge(userDAO);
                tx.commit();
                return userDAO;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteUser(String email) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                UserDAO userDAO = getUserByEmail(session, email);
                if (userDAO != null) {
                    session.remove(userDAO);
                    tx.commit();
                    LOGGER.info("Deleted user: {}", email);
                } else {
                    throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + email);
                }
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
