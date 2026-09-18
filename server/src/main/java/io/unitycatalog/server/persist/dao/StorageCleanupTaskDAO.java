package io.unitycatalog.server.persist.dao;

import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/** A pending cleanup task whose ID and name are copied from the deleted resource. */
@Entity
@Table(
    name = "uc_storage_cleanup_tasks",
    indexes = {
      @Index(name = "uc_storage_cleanup_tasks_deleted_idx", columnList = "deleted_at"),
      @Index(name = "uc_storage_cleanup_tasks_lease_idx", columnList = "lease_expires_at")
    })
@AttributeOverride(
    name = "id",
    column = @Column(name = "resource_id", updatable = false, nullable = false))
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StorageCleanupTaskDAO extends IdentifiableDAO {
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
