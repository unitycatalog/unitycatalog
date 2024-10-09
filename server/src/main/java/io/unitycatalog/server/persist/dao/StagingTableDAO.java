package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.StagingTableInfo;
import jakarta.persistence.*;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(name = "uc_staging_tables")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class StagingTableDAO extends IdentifiableDAO {

  @Column(name = "schema_id", columnDefinition = "BINARY(16)")
  private UUID schemaId;

  @Lob
  @Column(name = "staging_location", nullable = false)
  private String stagingLocation;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "accessed_at", nullable = false)
  private Date accessedAt;

  @Column(name = "stage_committed", nullable = false)
  private boolean stageCommitted;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "stage_committed_at")
  private Date stageCommittedAt;

  @Column(name = "purge_state", nullable = false)
  private short purgeState;

  @Column(name = "num_cleanup_retries", nullable = false)
  private short numCleanupRetries;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "last_cleanup_at")
  private Date lastCleanupAt;

  public StagingTableInfo toStagingTableInfo() {
    // TODO: populate metastore ID
    return new StagingTableInfo()
        .id(getId().toString())
        .stagingLocation(getStagingLocation())
        .name(getName());
  }

  public void setDefaultFields() {
    setCreatedAt(new Date()); // Assuming current date for creation
    setAccessedAt(new Date()); // Assuming current date for last access
    setStageCommitted(false);
    setStageCommittedAt(null);
    setPurgeState((short) 0);
    setNumCleanupRetries((short) 0);
    setLastCleanupAt(null);
  }
}
