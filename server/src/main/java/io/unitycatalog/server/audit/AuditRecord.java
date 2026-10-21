package io.unitycatalog.server.audit;

import io.unitycatalog.server.model.TableOperation;
import java.time.Instant;
import java.util.UUID;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * One audit record. It holds no secrets: no credential values, and storage locations without query,
 * fragment or user info. Fields that don't apply to the record's action are null.
 */
@Value
@Builder
public class AuditRecord {
  /** What the record records. */
  public enum Action {
    ISSUE_CREDENTIAL,
    GRANT,
    REVOKE,
    CLEAR_RESOURCE_GRANTS,
    CLEAR_PRINCIPAL_GRANTS,
    QUEUE_CLEANUP
  }

  /** Who a credential is issued for. */
  public enum Purpose {
    /** Returned to the API caller. */
    CLIENT,
    /** Used by the server itself while handling a request. */
    SERVER,
    /** Used by the background storage cleanup. */
    CLEANUP
  }

  @NonNull Instant time;
  @NonNull Action action;

  /** Acting user's email, {@code system:storage-cleanup}, {@code system:server}, or null. */
  String user;

  String requestId;
  Purpose purpose;
  String location;

  /** {@code READ} or {@code READ_WRITE}. */
  TableOperation access;

  /** When the credential expires. */
  Instant expiresAt;

  UUID resourceId;
  UUID principalId;

  /** Privilege in its public spelling, for example {@code USE CATALOG} or {@code OWNER}. */
  String privilege;

  /**
   * Declared so Javadoc, which runs without Lombok, can resolve the builder type; Lombok fills it.
   */
  public static class AuditRecordBuilder {}
}
