package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.GroupDAO;
import io.unitycatalog.server.persist.dao.GroupMemberDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Group;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.ConstraintViolationException.ConstraintKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups and their member users. Group names are unique across the server: the database's unique
 * constraint decides, so two concurrent requests for one name cannot both succeed.
 */
public class GroupRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupRepository.class);
  private final SessionFactory sessionFactory;

  public GroupRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Creates a group that {@code owner} manages, with {@code memberIds} in the owner's member list:
   * the admin's, or the IdP's.
   *
   * @throws BaseException {@code ALREADY_EXISTS} if the name is taken, {@code INVALID_ARGUMENT} if
   *     a member is not an existing user. Nothing is created in either case.
   */
  public Group createGroup(String name, String externalId, String owner, Set<UUID> memberIds) {
    boolean adminAdded = IdentityUtils.ADMIN_OWNER.equals(owner);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          requireUsers(session, memberIds, adminAdded ? null : owner);
          Date now = new Date();
          GroupDAO groupDAO =
              GroupDAO.builder()
                  .id(UUID.randomUUID())
                  .name(name)
                  .externalId(externalId)
                  .owner(owner)
                  .createdAt(now)
                  .build();
          session.persist(groupDAO);
          flushOrNameTaken(session, name);
          memberIds.forEach(
              memberId ->
                  session.persist(new GroupMemberDAO(groupDAO.getId(), memberId, adminAdded)));
          return toGroup(groupDAO, memberships(session, groupDAO.getId()));
        },
        "Failed to create group",
        /* readOnly= */ false);
  }

  public Group getGroup(String id) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          GroupDAO groupDAO = findGroup(session, id, /* lock= */ false);
          return toGroup(groupDAO, memberships(session, groupDAO.getId()));
        },
        "Failed to get group",
        /* readOnly= */ true);
  }

  /** All groups ordered by name, each with its members. */
  public List<Group> listGroups() {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Map<UUID, List<Membership>> membersByGroup =
              memberships(session, null).stream()
                  .collect(Collectors.groupingBy(Membership::groupId));
          return session.createQuery("FROM GroupDAO ORDER BY name", GroupDAO.class).list().stream()
              .map(dao -> toGroup(dao, membersByGroup.getOrDefault(dao.getId(), List.of())))
              .toList();
        },
        "Failed to list groups",
        /* readOnly= */ true);
  }

  /**
   * Replaces a group by what {@code change} returns for it, all in one transaction: the name, the
   * external id and both member lists. {@code change} must not throw anything but a {@link
   * BaseException}.
   *
   * @throws BaseException {@code NOT_FOUND} if there is no such group, {@code ALREADY_EXISTS} if
   *     the new name is taken, {@code INVALID_ARGUMENT} if a new member is not an existing user.
   *     Nothing changes in these cases.
   */
  public Group updateGroup(String id, UnaryOperator<Group> change) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          GroupDAO groupDAO = findGroup(session, id, /* lock= */ true);
          Group current = toGroup(groupDAO, memberships(session, groupDAO.getId()));
          Group updated = change.apply(current);

          for (boolean adminAdded : List.of(false, true)) {
            Set<UUID> added = new LinkedHashSet<>(updated.getMemberIds(adminAdded));
            added.removeAll(current.getMemberIds(adminAdded));
            // Only the IdP that owns the group writes its IdP list.
            requireUsers(session, added, adminAdded ? null : groupDAO.getOwner());
          }

          groupDAO.setName(updated.getName());
          groupDAO.setExternalId(updated.getExternalId());
          groupDAO.setUpdatedAt(new Date());
          flushOrNameTaken(session, updated.getName());
          for (boolean adminAdded : List.of(false, true)) {
            replaceList(
                session,
                groupDAO.getId(),
                adminAdded,
                current.getMemberIds(adminAdded),
                updated.getMemberIds(adminAdded));
          }
          return toGroup(groupDAO, memberships(session, groupDAO.getId()));
        },
        "Failed to update group",
        /* readOnly= */ false);
  }

  /** Deletes the group and its memberships; the users stay. */
  public void deleteGroup(String id) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          GroupDAO groupDAO = findGroup(session, id, /* lock= */ true);
          session
              .createMutationQuery("DELETE FROM GroupMemberDAO WHERE groupId = :groupId")
              .setParameter("groupId", groupDAO.getId())
              .executeUpdate();
          session.remove(groupDAO);
          LOGGER.info("Deleted group: {}", id);
          return null;
        },
        "Failed to delete group",
        /* readOnly= */ false);
  }

  /**
   * Finds the group. A write locks its row until the transaction ends, so concurrent changes to one
   * group, and a delete next to a change, run one after the other.
   */
  private GroupDAO findGroup(Session session, String id, boolean lock) {
    GroupDAO groupDAO = null;
    try {
      groupDAO =
          session
              .createSelectionQuery("FROM GroupDAO WHERE id = :id", GroupDAO.class)
              .setParameter("id", UUID.fromString(id))
              .setHibernateLockMode(lock ? LockMode.PESSIMISTIC_WRITE : LockMode.NONE)
              .getSingleResultOrNull();
    } catch (IllegalArgumentException e) {
      // Not a UUID, so no group has this id.
    }
    if (groupDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Group not found: " + id);
    }
    return groupDAO;
  }

  /** Writes the changes from {@code current} to {@code updated} in one of the member lists. */
  private static void replaceList(
      Session session, UUID groupId, boolean adminAdded, Set<UUID> current, Set<UUID> updated) {
    Set<UUID> removed = new HashSet<>(current);
    removed.removeAll(updated);
    if (!removed.isEmpty()) {
      session
          .createMutationQuery(
              "DELETE FROM GroupMemberDAO WHERE groupId = :groupId AND adminAdded = :adminAdded"
                  + " AND userId IN :userIds")
          .setParameter("groupId", groupId)
          .setParameter("adminAdded", adminAdded)
          .setParameterList("userIds", removed)
          .executeUpdate();
    }
    updated.stream()
        .filter(userId -> !current.contains(userId))
        .forEach(userId -> session.persist(new GroupMemberDAO(groupId, userId, adminAdded)));
  }

  /** One membership row, with its user's email. */
  private record Membership(UUID groupId, UUID userId, boolean adminAdded, String email) {}

  /** The membership rows of the group {@code groupId}, or of every group when it is null. */
  private static List<Membership> memberships(Session session, UUID groupId) {
    var query =
        session.createQuery(
            "SELECT m.groupId, m.userId, m.adminAdded, u.email FROM GroupMemberDAO m, UserDAO u"
                + " WHERE u.id = m.userId"
                + (groupId == null ? "" : " AND m.groupId = :groupId"),
            Object[].class);
    if (groupId != null) {
      query.setParameter("groupId", groupId);
    }
    return query.list().stream()
        .map(row -> new Membership((UUID) row[0], (UUID) row[1], (Boolean) row[2], (String) row[3]))
        .toList();
  }

  /**
   * Members are users only: an unknown id, or the id of a group, is refused. The users are locked
   * until the transaction ends, so a user deleted at the same time either waits for this change and
   * then drops the new membership too, or is gone before it is checked.
   *
   * <p>An IdP ({@code idpOwner} not null) can only name the users it created: another owner's user
   * is not an existing user for it, as it is not found when the IdP looks it up.
   */
  private void requireUsers(Session session, Collection<UUID> userIds, String idpOwner) {
    if (userIds.isEmpty()) {
      return;
    }
    // TODO(#PR3): a deleted user is DISABLED like a deactivated one, so it can still be added;
    // drop this once user delete removes the row.
    var query =
        session
            .createSelectionQuery(
                "FROM UserDAO WHERE id IN :ids" + (idpOwner == null ? "" : " AND owner = :owner"),
                UserDAO.class)
            .setParameterList("ids", userIds)
            .setHibernateLockMode(LockMode.PESSIMISTIC_READ);
    if (idpOwner != null) {
      query.setParameter("owner", idpOwner);
    }
    // MySQL at its default collation also matches owners that differ in case or accents.
    Set<UUID> found =
        new HashSet<>(
            query.list().stream()
                .filter(user -> idpOwner == null || idpOwner.equals(user.getOwner()))
                .map(UserDAO::getId)
                .toList());
    userIds.stream()
        .filter(userId -> !found.contains(userId))
        .findFirst()
        .ifPresent(
            userId -> {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT, "Member is not an existing user: " + userId);
            });
  }

  // TODO(#PR3): use UniqueViolations.flushOrAlreadyExists.
  /**
   * Writes the pending changes now, so a taken name fails here and not at commit. Hibernate marks
   * the violation as UNIQUE on H2 and MySQL but not on PostgreSQL, so SQLSTATE 23505 (unique
   * violation on PostgreSQL and H2) is checked as well.
   */
  private void flushOrNameTaken(Session session, String name) {
    try {
      session.flush();
    } catch (ConstraintViolationException e) {
      if (e.getKind() == ConstraintKind.UNIQUE || "23505".equals(e.getSQLState())) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Group already exists: " + name, e);
      }
      throw e;
    }
  }

  private Group toGroup(GroupDAO groupDAO, List<Membership> members) {
    Set<UUID> idpMemberIds = new LinkedHashSet<>();
    Set<UUID> adminMemberIds = new LinkedHashSet<>();
    Map<UUID, String> memberEmails = new HashMap<>();
    for (Membership member : members) {
      (member.adminAdded() ? adminMemberIds : idpMemberIds).add(member.userId());
      memberEmails.put(member.userId(), member.email());
    }
    return Group.builder()
        .id(groupDAO.getId())
        .name(groupDAO.getName())
        .externalId(groupDAO.getExternalId())
        .owner(groupDAO.getOwner())
        .idpMemberIds(idpMemberIds)
        .adminMemberIds(adminMemberIds)
        .memberEmails(memberEmails)
        .createdAt(groupDAO.getCreatedAt())
        .updatedAt(groupDAO.getUpdatedAt())
        .build();
  }
}
