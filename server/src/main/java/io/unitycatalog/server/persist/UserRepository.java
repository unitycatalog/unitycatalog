package io.unitycatalog.server.persist;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.persist.model.UserLink;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.query.MutationQuery;
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
            .owner(createUser.getOwner())
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
          if (updateUser.getName() != null) {
            userDAO.setName(updateUser.getName());
          }
          if (updateUser.getActive() != null) {
            // Reactivation clears the link: the IdP may reuse the row for a new person (a rehire),
            // so the next first call links whoever the IdP now names by this email, with a token
            // issued since (see linkUser).
            if (updateUser.getActive()
                && User.StateEnum.DISABLED.toString().equals(userDAO.getState())) {
              userDAO.setLinkIssuer(null);
              userDAO.setLinkClaim(null);
              userDAO.setLinkValue(null);
              userDAO.setReactivatedAt(Date.from(Instant.now().truncatedTo(ChronoUnit.SECONDS)));
            }
            userDAO.setState(
                updateUser.getActive()
                    ? User.StateEnum.ENABLED.toString()
                    : User.StateEnum.DISABLED.toString());
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
            userDAO.setExternalId(updateUser.getExternalId());
          }
          session.merge(userDAO);
          return userDAO.toUser();
        },
        "Failed to update user",
        /* readOnly= */ false);
  }

  /**
   * Marks the user as the principal of the identity provider {@code idpName}, for its SCIM token.
   * Refuses the admin user, a user an identity provider owns (only the admin manages a principal),
   * and a user that is already the principal of another identity provider.
   */
  public User setIdpName(String email, String idpName) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserByEmail(session, email);
          if (userDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + email);
          }
          // The stored email, so that a case variant of "admin" is refused as well.
          if ("admin".equals(userDAO.getEmail())) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT, "The admin user can't be an IdP principal.");
          }
          if (!IdentityUtils.ADMIN_OWNER.equals(userDAO.getOwner())) {
            throw new BaseException(
                ErrorCode.FAILED_PRECONDITION,
                String.format(
                    "%s is managed by identity provider %s; the SCIM principal must be a user the"
                        + " admin owns",
                    email, userDAO.getOwner()));
          }
          if (userDAO.getIdpName() != null && !userDAO.getIdpName().equals(idpName)) {
            throw new BaseException(
                ErrorCode.FAILED_PRECONDITION,
                String.format(
                    "User %s is already the IdP principal of %s, not %s.",
                    email, userDAO.getIdpName(), idpName));
          }
          userDAO.setIdpName(idpName);
          session.merge(userDAO);
          return userDAO.toUser();
        },
        "Failed to set the IdP name of the user",
        /* readOnly= */ false);
  }

  /** Returns the user linked to {@code link}, or null if no row is linked to it. */
  public User findLinkedUser(UserLink link) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Query<UserDAO> query =
              session.createQuery(
                  "FROM UserDAO WHERE linkIssuer = :issuer AND linkClaim = :claim"
                      + " AND linkValue = :value",
                  UserDAO.class);
          query.setParameter("issuer", link.issuer());
          query.setParameter("claim", link.claim());
          query.setParameter("value", link.value());
          // A MySQL database at its default collation compares case- and accent-insensitively, so
          // the query can return a row for another identity; only an exact match is the link.
          return query.list().stream()
              .filter(
                  userDAO ->
                      link.equals(
                          new UserLink(
                              userDAO.getLinkIssuer(),
                              userDAO.getLinkClaim(),
                              userDAO.getLinkValue())))
              .findFirst()
              .map(UserDAO::toUser)
              .orElse(null);
        },
        "Failed to get user by link",
        /* readOnly= */ true);
  }

  /**
   * Links {@code link} to the user with {@code email}, in one conditional update, so concurrent
   * first calls link the row at most once. Only an ENABLED, unlinked row that is neither the
   * bootstrap admin nor an IdP principal is linked. A reactivated row is linked only by a token
   * issued at or after its reactivation, so a token issued before it (or without {@code iat}) can't
   * link the row first. Both times have whole seconds, so a token issued in the second of the
   * reactivation links it.
   *
   * @param issuedAt the token's {@code iat}, or null if it has none
   * @return the linked user, or null if no such row exists or {@code link} is already on another
   *     row
   */
  public User linkUser(String email, UserLink link, Date issuedAt) {
    try {
      return TransactionManager.executeWithTransaction(
          sessionFactory,
          session -> {
            MutationQuery update =
                session
                    .createMutationQuery(
                        "UPDATE UserDAO SET linkIssuer = :issuer, linkClaim = :claim,"
                            + " linkValue = :value WHERE email = :email AND linkIssuer IS NULL"
                            + " AND state = :enabled AND email <> 'admin' AND idpName IS NULL"
                            + (issuedAt == null
                                ? " AND reactivatedAt IS NULL"
                                : " AND (reactivatedAt IS NULL OR reactivatedAt <= :issuedAt)"))
                    .setParameter("issuer", link.issuer())
                    .setParameter("claim", link.claim())
                    .setParameter("value", link.value())
                    .setParameter("email", email)
                    .setParameter("enabled", User.StateEnum.ENABLED.toString());
            if (issuedAt != null) {
              update.setParameter("issuedAt", issuedAt);
            }
            int linked = update.executeUpdate();
            return linked == 0 ? null : getUserByEmail(session, email).toUser();
          },
          "Failed to link user",
          /* readOnly= */ false);
    } catch (RuntimeException e) {
      // A concurrent call linked this identity to another row (uc_users_link_key).
      if (ExceptionUtils.indexOfType(e, ConstraintViolationException.class) < 0) {
        throw e;
      }
      return null;
    }
  }

  public void deleteUser(String id) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO userDAO = getUserById(session, id);
          if (userDAO != null) {
            userDAO.setState(User.StateEnum.DISABLED.toString());
            session.merge(userDAO);
            // Write the user row before the memberships: a group add reads it and then adds a
            // membership, so the other order can deadlock with that add.
            session.flush();
            session
                .createMutationQuery("DELETE FROM GroupMemberDAO WHERE userId = :userId")
                .setParameter("userId", userDAO.getId())
                .executeUpdate();
            LOGGER.info("Deleted user: {}", id);
            return null;
          } else {
            throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + id);
          }
        },
        "Failed to delete user",
        /* readOnly= */ false);
  }

  public UUID findPrincipalId() {
    String principalEmailAddress = IdentityUtils.findPrincipalEmailAddress();
    if (principalEmailAddress == null) {
      return null;
    }
    // A SCIM token's subject is already the user's id; AuthDecorator looked the user up by it.
    if (IdentityUtils.isScimToken()) {
      return UUID.fromString(principalEmailAddress);
    }
    return UUID.fromString(getUserByEmail(principalEmailAddress).getId());
  }
}
