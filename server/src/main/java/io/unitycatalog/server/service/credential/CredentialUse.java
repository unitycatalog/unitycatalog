package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.audit.AuditRecord.Purpose;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Who a vended storage credential is for, and, for the storage cleanup, which dropped object. It
 * only labels the audit record; it does not change what is vended. Only {@link #CLIENT}, {@link
 * #SERVER} and {@link #cleanup(UUID)} exist, so a purpose and a resource ID cannot disagree.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CredentialUse {
  /** A credential returned to the API caller. */
  public static final CredentialUse CLIENT = new CredentialUse(Purpose.CLIENT, null);

  /** A credential the server uses itself while handling a request. */
  public static final CredentialUse SERVER = new CredentialUse(Purpose.SERVER, null);

  Purpose purpose;

  /** The dropped object's ID on a cleanup credential, otherwise null. */
  UUID resourceId;

  /** A credential the storage cleanup uses to delete the dropped object {@code resourceId}. */
  public static CredentialUse cleanup(UUID resourceId) {
    if (resourceId == null) {
      throw new IllegalArgumentException("A cleanup credential needs the dropped object's ID");
    }
    return new CredentialUse(Purpose.CLEANUP, resourceId);
  }
}
