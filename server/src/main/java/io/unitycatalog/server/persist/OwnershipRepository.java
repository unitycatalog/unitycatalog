package io.unitycatalog.server.persist;

import io.unitycatalog.server.audit.Auditor;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.LockMode;
import org.hibernate.Session;

/** Writes ownership policies in the transaction that updates the resource's owner field. */
public class OwnershipRepository {
  private final UserRepository userRepository;
  private final boolean authorizationEnabled;
  private final Optional<Auditor> auditor;

  OwnershipRepository(
      UserRepository userRepository, boolean authorizationEnabled, Optional<Auditor> auditor) {
    this.userRepository = userRepository;
    this.authorizationEnabled = authorizationEnabled;
    this.auditor = auditor;
  }

  /**
   * Locks and refreshes the resource before any metadata changes, replaces its OWNER policy, and
   * returns the new owner's username. The caller must set the owner field in this transaction and
   * reload the authorizer after commit. Concurrent metadata updates must not write unchanged owner
   * fields.
   *
   * @param session the resource's write transaction
   * @param resource the existing resource, before applying the request
   * @param owner the requested username
   * @return the stored username
   */
  public String updateOwner(Session session, IdentifiableDAO resource, String owner) {
    if (owner == null || owner.isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "New owner must be specified.");
    }
    UserDAO user = userRepository.getUserByEmail(session, owner);
    if (user == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + owner);
    }
    session.refresh(resource, LockMode.PESSIMISTIC_WRITE);
    if (authorizationEnabled) {
      auditor.ifPresent(
          a -> {
            List<UUID> oldOwners =
                session
                    .createNativeQuery(
                        "SELECT v0 FROM casbin_rule WHERE ptype = 'p' AND v1 = :resource AND"
                            + " v2 = :owner",
                        String.class)
                    .setParameter("resource", resource.getId().toString())
                    .setParameter("owner", Privileges.OWNER.getValue())
                    .getResultList()
                    .stream()
                    .map(UUID::fromString)
                    .toList();
            a.recordOwnershipTransfer(session, resource.getId(), oldOwners, user.getId());
          });
      // The JDBC adapter commits on a separate connection. Use this transaction for both writes.
      session
          .createNativeMutationQuery(
              "DELETE FROM casbin_rule WHERE ptype = 'p' AND v1 = :resource AND v2 = :owner")
          .setParameter("resource", resource.getId().toString())
          .setParameter("owner", Privileges.OWNER.getValue())
          .executeUpdate();
      // Insert a new row so the existing policy refresher also observes same-size transfers.
      session
          .createNativeMutationQuery(
              "INSERT INTO casbin_rule (ptype, v0, v1, v2) VALUES ('p', :principal, :resource,"
                  + " :owner)")
          .setParameter("principal", user.getId().toString())
          .setParameter("resource", resource.getId().toString())
          .setParameter("owner", Privileges.OWNER.getValue())
          .executeUpdate();
    }
    return user.getEmail();
  }
}
