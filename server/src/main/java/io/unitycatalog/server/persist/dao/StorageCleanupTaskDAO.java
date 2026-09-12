package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(
    name = "uc_storage_cleanup_tasks",
    indexes = {@Index(name = "uc_storage_cleanup_tasks_ready_idx", columnList = "cleanable_at")})
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageCleanupTaskDAO {
  public enum ResourceType {
    TABLE,
    VOLUME,
    REGISTERED_MODEL,
    MODEL_VERSION,
    STAGING_TABLE
  }

  @Enumerated(EnumType.STRING)
  @Column(name = "resource_type", nullable = false)
  private ResourceType resourceType;

  @Id
  @Column(name = "resource_id", updatable = false, nullable = false)
  private UUID resourceId;

  @Column(name = "storage_location", length = 4096, nullable = false)
  private String storageLocation;

  @JdbcTypeCode(SqlTypes.TIMESTAMP_UTC)
  @Column(name = "cleanable_at", nullable = false)
  private Instant cleanableAt;

  @Column(name = "lease_token")
  private UUID leaseToken;

  @JdbcTypeCode(SqlTypes.TIMESTAMP_UTC)
  @Column(name = "lease_expires_at")
  private Instant leaseExpiresAt;

  @Column(name = "failure_count", nullable = false)
  private int failureCount;

  @Column(name = "last_error", length = 2048)
  private String lastError;
}
