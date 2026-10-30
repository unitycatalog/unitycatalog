package io.unitycatalog.server.audit;

import io.unitycatalog.server.service.credential.Purpose;
import java.time.Instant;
import java.util.UUID;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * One audit record. It holds no secrets: no credential values, and storage locations without query
 * or fragment, and without user info except an ABFS container name before the {@code @}. Fields
 * that don't apply to the record's action are null.
 */
@Value
@Builder
public class AuditRecord {
  /** What the record records. */
  public enum Action {
    ISSUE_CREDENTIAL
  }

  /** What a credential allows. */
  public enum Access {
    READ,
    READ_WRITE
  }

  @NonNull Instant time;
  @NonNull Action action;

  /**
   * The caller's token subject (an email, {@code admin} or a service name), {@code
   * system:storage-cleanup}, or null.
   */
  String user;

  String requestId;
  Purpose purpose;
  String location;

  Access access;

  /** When the credential expires. */
  Instant expiresAt;

  UUID resourceId;

  /**
   * Declared so Javadoc, which runs without Lombok, can resolve the builder type; Lombok fills it.
   */
  public static class AuditRecordBuilder {}
}
