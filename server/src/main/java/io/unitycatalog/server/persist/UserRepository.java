package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateUser;
import io.unitycatalog.server.model.UpdateUser;
import io.unitycatalog.server.model.User;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserRepository {
  private static final UserRepository INSTANCE = new UserRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final PagedListingHelper<UserDAO> LISTING_HELPER =
      new PagedListingHelper<>(UserDAO.class);

  private UserRepository() {}

  public static UserRepository getInstance() {
    return INSTANCE;
  }

  public User createUser(CreateUser createUser) {
    User user =
        new User()
            .id(UUID.randomUUID().toString())
            .name(createUser.getName())
            .email(createUser.getEmail())
            .externalId(createUser.getExternalId())
            .state(User.StateEnum.ENABLED)
            .createdAt(System.currentTimeMillis());

    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        if (getUserByName(session, user.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS, "User already exists: " + user.getName());
        }
        session.persist(UserDAO.from(user));
        tx.commit();
        return user;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public List<User> listUsers() {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        List<UserDAO> userDAOs =
            LISTING_HELPER.listEntity(session, Optional.empty(), Optional.empty(), null);
        tx.commit();
        return userDAOs.stream().map(UserDAO::toUser).collect(Collectors.toList());
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public User getUser(String name) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        UserDAO userDAO = getUserByName(session, name);
        if (userDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + name);
        }
        tx.commit();
        return userDAO.toUser();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public UserDAO getUserByName(Session session, String name) {
    Query<UserDAO> query = session.createQuery("FROM UserDAO WHERE name = :name", UserDAO.class);
    query.setParameter("name", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public User updateUser(String name, UpdateUser updateUser) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        UserDAO userDAO = getUserByName(session, name);
        if (userDAO == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + name);
        }
        if (updateUser.getNewName() != null) {
          userDAO.setName(updateUser.getNewName());
        }
        if (updateUser.getEmail() != null) {
          userDAO.setEmail(updateUser.getEmail());
        }
        if (updateUser.getExternalId() != null) {
          userDAO.setExternalId(updateUser.getExternalId());
        }
        session.merge(userDAO);
        tx.commit();
        return userDAO.toUser();
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
        UserDAO userDAO = getUserByName(session, email);
        if (userDAO != null) {
          userDAO.setState(User.StateEnum.DISABLED.toString());
          session.merge(userDAO);
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
