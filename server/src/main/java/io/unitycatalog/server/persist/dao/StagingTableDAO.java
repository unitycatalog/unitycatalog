package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.StagingTableInfo;
import jakarta.persistence.*;
import java.util.Date;
import java.util.UUID;
import lombok.*;

// Hibernate annotations
@Entity
@Table(name = "uc_staging_tables")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class StagingTableDAO {
  @Id
  @Column(name = "id", nullable = false)
  private UUID id;

  @Column(name = "schema_id", columnDefinition = "BINARY(16)")
  private UUID schemaId;

  @Column(name = "name")
  private String name;

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

  public static StagingTableDAO from(StagingTableInfo dto) {
    if (dto == null) {
      return null;
    }
    StagingTableDAO dao =
        StagingTableDAO.builder().stagingLocation(dto.getStagingLocation()).build();
    if (dto.getId() != null) {
      dao.setId(UUID.fromString(dto.getId()));
    }
    return dao;
  }

  public StagingTableInfo toStagingTableInfo() {
    StagingTableInfo dto = new StagingTableInfo().stagingLocation(getStagingLocation());
    if (getId() != null) {
      dto.id(getId().toString());
    }
    return dto;
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
