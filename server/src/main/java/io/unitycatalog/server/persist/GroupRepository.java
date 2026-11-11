package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.GroupDAO;
import io.unitycatalog.server.persist.dao.GroupMemberDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Group;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.util.Collection;
import java.util.Date;
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
   * @throws BaseException {@code ALREADY_EXISTS} if the name is taken, {@code INVALID_ARGUMENT} if
   *     a member is not an existing user. Nothing is created in either case.
   */
  public Group createGroup(String name, String externalId, Set<UUID> memberIds) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          requireUsers(session, memberIds);
          Date now = new Date();
          GroupDAO groupDAO =
              GroupDAO.builder()
                  .id(UUID.randomUUID())
                  .name(name)
                  .externalId(externalId)
                  .createdAt(now)
                  .build();
          session.persist(groupDAO);
          flushOrNameTaken(session, name);
          memberIds.forEach(
              memberId -> session.persist(new GroupMemberDAO(groupDAO.getId(), memberId)));
          return toGroup(groupDAO, memberIds);
        },
        "Failed to create group",
        /* readOnly= */ false);
  }

  public Group getGroup(String id) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          GroupDAO groupDAO = findGroup(session, id, /* lock= */ false);
          return toGroup(groupDAO, memberIdsOf(session, groupDAO.getId()));
        },
        "Failed to get group",
        /* readOnly= */ true);
  }

  /** All groups ordered by name, each with its members. */
  public List<Group> listGroups() {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Map<UUID, Set<UUID>> membersByGroup =
              session.createQuery("FROM GroupMemberDAO", GroupMemberDAO.class).list().stream()
                  .collect(
                      Collectors.groupingBy(
                          GroupMemberDAO::getGroupId,
                          Collectors.mapping(GroupMemberDAO::getUserId, Collectors.toSet())));
          return session.createQuery("FROM GroupDAO ORDER BY name", GroupDAO.class).list().stream()
              .map(dao -> toGroup(dao, membersByGroup.getOrDefault(dao.getId(), Set.of())))
              .toList();
        },
        "Failed to list groups",
        /* readOnly= */ true);
  }

  /**
   * Replaces a group by what {@code change} returns for it, all in one transaction: the name, the
   * external id and the members. {@code change} must not throw anything but a {@link
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
          Set<UUID> current = memberIdsOf(session, groupDAO.getId());
          Group updated = change.apply(toGroup(groupDAO, current));

          Set<UUID> added = new LinkedHashSet<>(updated.getMemberIds());
          added.removeAll(current);
          Set<UUID> removed = new HashSet<>(current);
          removed.removeAll(updated.getMemberIds());
          requireUsers(session, added);

          groupDAO.setName(updated.getName());
          groupDAO.setExternalId(updated.getExternalId());
          groupDAO.setUpdatedAt(new Date());
          flushOrNameTaken(session, updated.getName());
          if (!removed.isEmpty()) {
            session
                .createMutationQuery(
                    "DELETE FROM GroupMemberDAO WHERE groupId = :groupId AND userId IN :userIds")
                .setParameter("groupId", groupDAO.getId())
                .setParameterList("userIds", removed)
                .executeUpdate();
          }
          added.forEach(userId -> session.persist(new GroupMemberDAO(groupDAO.getId(), userId)));
          return toGroup(groupDAO, updated.getMemberIds());
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

  private Set<UUID> memberIdsOf(Session session, UUID groupId) {
    return new LinkedHashSet<>(
        session
            .createQuery("SELECT userId FROM GroupMemberDAO WHERE groupId = :groupId", UUID.class)
            .setParameter("groupId", groupId)
            .list());
  }

  /**
   * Members are users only: an unknown id, or the id of a group, is refused. The users are locked
   * until the transaction ends, so a user deleted at the same time either waits for this change and
   * then drops the new membership too, or is gone before it is checked.
   */
  private void requireUsers(Session session, Collection<UUID> userIds) {
    if (userIds.isEmpty()) {
      return;
    }
    // TODO: a deleted user is DISABLED like a deactivated one, so it can still be added; drop this
    // once user delete removes the row.
    Set<UUID> found =
        new HashSet<>(
            session
                .createSelectionQuery("FROM UserDAO WHERE id IN :ids", UserDAO.class)
                .setParameterList("ids", userIds)
                .setHibernateLockMode(LockMode.PESSIMISTIC_READ)
                .list()
                .stream()
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

  private Group toGroup(GroupDAO groupDAO, Set<UUID> memberIds) {
    return Group.builder()
        .id(groupDAO.getId())
        .name(groupDAO.getName())
        .externalId(groupDAO.getExternalId())
        .memberIds(memberIds)
        .createdAt(groupDAO.getCreatedAt())
        .updatedAt(groupDAO.getUpdatedAt())
        .build();
  }
}
