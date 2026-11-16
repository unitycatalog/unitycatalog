package io.unitycatalog.server.persist;

import io.unitycatalog.server.audit.Auditor;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
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
   * Locks and refreshes the resource before any metadata changes, replaces its native OWNER grants
   * when authorization is enabled, and returns the new owner's username. The caller must set the
   * owner field in this transaction and reload the authorizer after commit. Concurrent metadata
   * updates must not write unchanged owner fields.
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
      List<UUID> oldOwners =
          JCasbinAuthorizer.replaceOwner(session, resource.getId(), user.getId());
      auditor.ifPresent(
          a -> a.recordOwnershipTransfer(session, resource.getId(), oldOwners, user.getId()));
    }
    return user.getEmail();
  }
}
