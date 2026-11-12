package io.unitycatalog.server.persist;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.persist.utils.UniqueViolations;
import io.unitycatalog.server.utils.IdentityUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<UserDAO> LISTING_HELPER =
      new PagedListingHelper<>(UserDAO.class);

  public UserRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
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

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          if (getUserByEmail(session, user.getEmail()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "User already exists: " + user.getEmail());
          }
          if (user.getExternalId() != null
              && getUserByExternalId(session, user.getExternalId()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "User already exists with external id: " + user.getExternalId());
          }
          session.persist(UserDAO.from(user));
          flushOrAlreadyExists(session, user.getEmail(), user.getExternalId());
          return user;
        },
        "Failed to create user",
        /* readOnly= */ false);
  }

  public List<User> listUsers(int startIndex, int maxUsers, Predicate<User> filter) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          int count = 0;
          List<User> users = new ArrayList<>();

          Optional<String> nextPageToken = Optional.empty();
          boolean hasMore = true;
          while (users.size() < maxUsers && hasMore) {
            List<UserDAO> userDAOs =
                LISTING_HELPER.listEntity(session, Optional.empty(), nextPageToken, null);

            List<User> userBlock =
                userDAOs.stream()
                    .map(UserDAO::toUser)
                    .filter(filter::test)
                    .collect(Collectors.toList());

            if (count + userBlock.size() < startIndex) {
              // if we haven't reached the start index, skip the block
              count += userBlock.size();
            } else if (count >= startIndex) {
              // we've already reached the start index, add the whole block
              users.addAll(userBlock);
              count += userBlock.size();
            } else {
              // we'll reach the start index in this block somewhere.
              int firstIndex = startIndex - count;
              users.addAll(userBlock.subList(firstIndex, userBlock.size()));
              count += userBlock.size();
            }
            nextPageToken =
                Optional.ofNullable(
                    userDAOs.isEmpty() ? null : userDAOs.get(userDAOs.size() - 1).getName());
            hasMore = nextPageToken.isPresent();
          }

          return users.subList(0, Math.min(users.size(), maxUsers));
        },
        "Failed to list users",
        /* readOnly= */ true);
  }

  public User getUser(String id) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserById(session, id);
          if (userDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + id);
          }
          return userDAO.toUser();
        },
        "Failed to get user",
        /* readOnly= */ true);
  }

  public UserDAO getUserById(Session session, String id) {
    Query<UserDAO> query = session.createQuery("FROM UserDAO WHERE id = :id", UserDAO.class);
    query.setParameter("id", UUID.fromString(id));
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  UUID findUserId(Session session, String email) {
    UserDAO userDAO = email == null ? null : getUserByEmail(session, email);
    return userDAO == null ? null : userDAO.getId();
  }

  /**
   * Whether the caller created a row. A row with a creator id belongs to that user, so a user
   * created again with the same email is not its creator. A row from before the id was stored has
   * only the creator's email, and is matched by it.
   */
  boolean isCreator(
      Session session, UUID createdById, String createdBy, String callerEmail) {
    if (createdById == null) {
      return Objects.equals(createdBy, callerEmail);
    }
    return createdById.equals(findUserId(session, callerEmail));
  }

  public User getUserByEmail(String email) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserByEmail(session, email);
          if (userDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + email);
          }
          return userDAO.toUser();
        },
        "Failed to get user by email",
        /* readOnly= */ true);
  }

  public User getUserByExternalId(String externalId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserByExternalId(session, externalId);
          if (userDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + externalId);
          }
          return userDAO.toUser();
        },
        "Failed to get user by external id",
        /* readOnly= */ true);
  }

  public UserDAO getUserByEmail(Session session, String email) {
    Query<UserDAO> query = session.createQuery("FROM UserDAO WHERE email = :email", UserDAO.class);
    query.setParameter("email", email);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public UserDAO getUserByExternalId(Session session, String externalId) {
    Query<UserDAO> query =
        session.createQuery("FROM UserDAO WHERE externalId = :externalId", UserDAO.class);
    query.setParameter("externalId", externalId);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public User updateUser(String id, UpdateUser updateUser) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserById(session, id);
          if (userDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + id);
          }
          // Check before changing anything: a query after a change flushes it outside
          // flushOrAlreadyExists, which would report a lost race as a server error.
          if (updateUser.getEmail() != null) {
            UserDAO existingEmailUser = getUserByEmail(session, updateUser.getEmail());
            if (existingEmailUser != null && !existingEmailUser.getId().equals(userDAO.getId())) {
              throw new BaseException(
                  ErrorCode.ALREADY_EXISTS, "User already exists: " + updateUser.getEmail());
            }
          }
          if (updateUser.getExternalId() != null) {
            UserDAO existingExternalIdUser =
                getUserByExternalId(session, updateUser.getExternalId());
            if (existingExternalIdUser != null
                && !existingExternalIdUser.getId().equals(userDAO.getId())) {
              throw new BaseException(
                  ErrorCode.ALREADY_EXISTS,
                  "User already exists with external id: " + updateUser.getExternalId());
            }
          }
          if (updateUser.getName() != null) {
            userDAO.setName(updateUser.getName());
          }
          if (updateUser.getActive() != null) {
            userDAO.setState(
                updateUser.getActive()
                    ? User.StateEnum.ENABLED.toString()
                    : User.StateEnum.DISABLED.toString());
          }
          if (updateUser.getEmail() != null) {
            userDAO.setEmail(updateUser.getEmail());
          }
          if (updateUser.getExternalId() != null) {
            userDAO.setExternalId(updateUser.getExternalId());
          }
          session.merge(userDAO);
          flushOrAlreadyExists(session, userDAO.getEmail(), userDAO.getExternalId());
          return userDAO.toUser();
        },
        "Failed to update user",
        /* readOnly= */ false);
  }

  public void deleteUser(String id) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserById(session, id);
          if (userDAO != null) {
            session.remove(userDAO);
            LOGGER.info("Deleted user: {}", id);
            return null;
          } else {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + id);
          }
        },
        "Failed to delete user",
        /* readOnly= */ false);
  }

  /**
   * Check-then-insert lets two requests pass the checks together, so the unique constraints decide
   * the race; this reports the loser with the message of the sequential duplicate.
   */
  private static void flushOrAlreadyExists(Session session, String email, String externalId) {
    UniqueViolations.flushOrAlreadyExists(
        session,
        Map.of(
            "uc_users_email_key", "User already exists: " + email,
            "uc_users_external_id_key", "User already exists with external id: " + externalId));
  }

  public UUID findPrincipalId() {
    String principalEmailAddress = IdentityUtils.findPrincipalEmailAddress();
    if (principalEmailAddress != null) {
      return UUID.fromString(getUserByEmail(principalEmailAddress).getId());
    } else {
      return null;
    }
  }
}
