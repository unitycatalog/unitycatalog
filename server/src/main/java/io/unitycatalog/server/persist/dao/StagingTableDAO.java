package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.StagingTableInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(
    name = "uc_staging_tables",
    indexes = {
      @Index(name = "idx_staging_location", columnList = "staging_location"),
    })
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class StagingTableDAO extends IdentifiableDAO {
  @Column(name = "schema_id")
  private UUID schemaId;

  @Column(name = "staging_location", length = 2048, nullable = false)
  private String stagingLocation;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "accessed_at", nullable = false)
  private Date accessedAt;

  @Column(name = "stage_committed", nullable = false)
  private boolean stageCommitted;

  @Column(name = "stage_committed_at")
  private Date stageCommittedAt;

  @Column(name = "purge_state", nullable = false)
  private short purgeState;

  @Column(name = "num_cleanup_retries", nullable = false)
  private short numCleanupRetries;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "last_cleanup_at")
  private Date lastCleanupAt;

  public StagingTableInfo toStagingTableInfo(String catalogName, String schemaName) {
    return new StagingTableInfo()
        .id(getId().toString())
        .stagingLocation(getStagingLocation())
        .name(getName())
        .catalogName(catalogName)
        .schemaName(schemaName);
  }

  public void setDefaultFields() {
    Date now = new Date();
    setCreatedAt(now); // Assuming current date for creation
    setAccessedAt(now); // Assuming current date for last access
    setStageCommitted(false);
    setStageCommittedAt(null);
    setPurgeState((short) 0);
    setNumCleanupRetries((short) 0);
    setLastCleanupAt(null);
  }
}
