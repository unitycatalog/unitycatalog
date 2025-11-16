package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.DeltaCommitInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(
    name = "uc_delta_commits",
    uniqueConstraints = {@UniqueConstraint(columnNames = {"table_id", "commit_version"})})
// Lombok
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DeltaCommitDAO {
  // This id is never used to identify a commit. We only look for commits by table_id and
  // optionally commit_version. But this is a required field as a unique ID in the database.
  @Id
  @UuidGenerator
  @Column(name = "id", columnDefinition = "BINARY(16)")
  private UUID id;

  @Column(name = "table_id", nullable = false, columnDefinition = "BINARY(16)")
  private UUID tableId;

  @Column(name = "commit_version", nullable = false)
  private long commitVersion;

  @Column(name = "commit_filename", nullable = false)
  private String commitFilename;

  @Column(name = "commit_filesize", nullable = false)
  private long commitFilesize;

  @Column(name = "commit_file_modification_timestamp", nullable = false)
  private Date commitFileModificationTimestamp;

  @Column(name = "commit_timestamp", nullable = false)
  private Date commitTimestamp;

  @Column(name = "is_backfilled_latest_commit", nullable = false)
  private boolean isBackfilledLatestCommit;

  public static DeltaCommitDAO from(UUID tableId, DeltaCommitInfo commitInfo) {
    return DeltaCommitDAO.builder()
        .tableId(tableId)
        .commitVersion(commitInfo.getVersion())
        .commitFilename(commitInfo.getFileName())
        .commitFilesize(commitInfo.getFileSize())
        .commitFileModificationTimestamp(new Date(commitInfo.getFileModificationTimestamp()))
        .commitTimestamp(new Date(commitInfo.getTimestamp()))
        .isBackfilledLatestCommit(false)
        .build();
  }
}
