package io.unitycatalog.server.audit;

import io.unitycatalog.server.model.TableOperation;
import java.time.Instant;
import java.util.UUID;
import lombok.Builder;
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
    ISSUE_CREDENTIAL
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

  Instant time;
  Action action;

  /** Acting user's email, {@code system:storage-cleanup}, {@code system:bootstrap}, or null. */
  String user;

  String requestId;
  Purpose purpose;
  String location;

  /** {@code READ} or {@code READ_WRITE}. */
  TableOperation access;

  /** Credential expiry in epoch milliseconds. */
  Long expiresAt;

  UUID resourceId;

  /**
   * Declared so Javadoc, which runs without Lombok, can resolve the builder type; Lombok fills it.
   */
  public static class AuditRecordBuilder {}
}
