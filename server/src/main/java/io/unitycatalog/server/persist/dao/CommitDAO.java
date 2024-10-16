package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.CommitInfo;
import jakarta.persistence.*;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(
    name = "uc_commits",
    uniqueConstraints = {
      @UniqueConstraint(columnNames = {"table_id", "commit_version"})
    })
// Lombok
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CommitDAO {
  @Id
  @UuidGenerator
  @Column(name = "id", columnDefinition = "BINARY(16)")
  private UUID id;

  @Column(name = "table_id", nullable = false, columnDefinition = "BINARY(16)")
  private UUID tableId;

  @Column(name = "commit_version", nullable = false)
  private Long commitVersion;

  @Column(name = "commit_filename", nullable = false)
  private String commitFilename;

  @Column(name = "commit_filesize", nullable = false)
  private Long commitFilesize;

  @Column(name = "commit_file_modification_timestamp", nullable = false)
  private Date commitFileModificationTimestamp;

  @Column(name = "commit_timestamp", nullable = false)
  private Date commitTimestamp;

  @Column(name = "is_backfilled_latest_commit", nullable = false)
  private Boolean isBackfilledLatestCommit;

  @Column(name = "is_disown_commit")
  private Boolean isDisownCommit;

  public CommitInfo toCommitInfo() {
    return new CommitInfo()
        .version(commitVersion)
        .fileName(commitFilename)
        .fileSize(commitFilesize)
        .fileModificationTimestamp(commitFileModificationTimestamp.getTime())
        .timestamp(commitTimestamp.getTime())
        .isDisownCommit(isDisownCommit);
  }

  public static CommitDAO from(Commit commit) {
    return CommitDAO.builder()
        .tableId(UUID.fromString(commit.getTableId()))
        .commitVersion(commit.getCommitInfo().getVersion())
        .commitFilename(commit.getCommitInfo().getFileName())
        .commitFilesize(commit.getCommitInfo().getFileSize())
        .commitFileModificationTimestamp(
            new Date(commit.getCommitInfo().getFileModificationTimestamp()))
        .commitTimestamp(new Date(commit.getCommitInfo().getTimestamp()))
        .isDisownCommit(
            commit.getCommitInfo().getIsDisownCommit() != null
                && commit.getCommitInfo().getIsDisownCommit())
        .isBackfilledLatestCommit(false)
        .build();
  }
}
