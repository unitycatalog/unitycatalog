package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(
    name = "uc_storage_cleanup_tasks",
    indexes = {
      @Index(name = "uc_storage_cleanup_tasks_deleted_idx", columnList = "deleted_at"),
      @Index(name = "uc_storage_cleanup_tasks_lease_idx", columnList = "lease_expires_at")
    })
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

  @Column(name = "deleted_at", nullable = false)
  private Date deletedAt;

  /** Active lease expiry when a token exists; earliest retry time otherwise. */
  @Column(name = "lease_expires_at")
  private Date leaseExpiresAt;

  @Column(name = "lease_token")
  private UUID leaseToken;

  @Column(name = "failure_count", nullable = false)
  private int failureCount;

  @Column(name = "last_error", length = 2048)
  private String lastError;
}
