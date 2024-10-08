package io.unitycatalog.server.handler;

import static io.unitycatalog.server.persist.CommitRepository.MAX_NUM_COMMITS_PER_TABLE;

import io.unitycatalog.server.exception.CommitException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatedCommitsHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedCommitsHandler.class);
  public static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();
  public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();

  public static void validateCommit(Commit commit) {
    // We do not support disown commits
    if (commit.getCommitInfo() != null && commit.getCommitInfo().getIsDisownCommit()) {
      throw new CommitException(ErrorCode.UNIMPLEMENTED, "Disown commits are not supported!");
    }
    // Validate the commit object
    assert commit.getTableId() != null;
    //    assert commit.getTableUri() != null;

    // TODO: Add other assertions like the table URI path exists

    // Validate the commit info object
    if (commit.getCommitInfo() != null) {
      assert commit.getCommitInfo().getIsDisownCommit() != null;
      assert commit.getCommitInfo().getFileSize() > 0;
      assert commit.getCommitInfo().getFileName() != null;
      assert commit.getCommitInfo().getVersion() != null;
      assert commit.getCommitInfo().getTimestamp() != null;
      assert commit.getCommitInfo().getFileModificationTimestamp() != null;
    } else {
      // If commit info is null, then it should be a backfill only commit
      assert commit.getLatestBackfilledVersion() != null;
    }
  }

  public static void validateCommitTable(Commit commit) {
    TableInfo tableInfo = TABLE_REPOSITORY.getTableById(commit.getTableId());
    //    assert tableInfo.getTableType() == TableType.MANAGED;
    assert tableInfo.getDataSourceFormat() == DataSourceFormat.DELTA;

    // TODO: Add other assertions like verifying the table path (tableInfo.getStorageLocation)
  }

  public static void validateOnboardingCommit(Commit commit) {
    assert commit.getCommitInfo() != null;
    // Onboarding commit cannot be a disown commit
    assert !commit.getCommitInfo().getIsDisownCommit();
  }

  public static void handleFirstCommit(Session session, Commit commit) {
    validateOnboardingCommit(commit);
    COMMIT_REPOSITORY.saveCommit(session, commit);
  }

  public static void handleBackfillOnlyCommit(
      Session session,
      String tableId,
      Long latestBackfilledVersion,
      CommitDAO firstCommit,
      CommitDAO lastCommit) {
    // Nothing to delete, return
    if (latestBackfilledVersion < firstCommit.getCommitVersion()) {
      return;
    }
    // We only need to delete when there is more than one commit. We always keep the last commit.
    if (firstCommit.getCommitVersion() < lastCommit.getCommitVersion()) {
      COMMIT_REPOSITORY.backfillCommits(
          session,
          UUID.fromString(tableId),
          Math.min(latestBackfilledVersion, lastCommit.getCommitVersion() - 1),
          firstCommit,
          lastCommit.getCommitVersion());
    }
    if (latestBackfilledVersion == lastCommit.getCommitVersion()) {
      // Mark the last commit as the latest backfilled version
      COMMIT_REPOSITORY.markCommitAsLatestBackfilled(
          session, UUID.fromString(tableId), lastCommit.getCommitVersion());
    }

    // TODO: Should we also retain the disown commit if it's backfilled?
  }

  public static void handleReboardCommit() {
    throw new CommitException(ErrorCode.UNIMPLEMENTED, "Reboarding of tables not allowed!");
  }

  public static void handleNormalCommit(
      Session session, Commit commit, CommitDAO firstCommit, CommitDAO lastCommit) {
    if (commit.getCommitInfo().getVersion() <= lastCommit.getCommitVersion()) {
      throw new CommitException(
          ErrorCode.ALREADY_EXISTS,
          "Commit version should be greater than the last commit version = "
              + lastCommit.getCommitVersion());
    }
    if (commit.getCommitInfo().getVersion() > lastCommit.getCommitVersion() + 1) {
      throw new CommitException(
          ErrorCode.INVALID_ARGUMENT,
          "Commit version should be the next version after the last commit version = "
              + lastCommit.getCommitVersion());
    }
    if (commit.getLatestBackfilledVersion() != null
        && commit.getLatestBackfilledVersion() > lastCommit.getCommitVersion()) {
      throw new CommitException(
          ErrorCode.INVALID_ARGUMENT,
          "Latest backfilled version cannot be greater than the last commit version = "
              + lastCommit.getCommitVersion());
    }
    long latestBackfilledVersion = getLatestBackfilledVersion(commit, firstCommit, lastCommit);
    if (commit.getCommitInfo().getVersion() - latestBackfilledVersion > MAX_NUM_COMMITS_PER_TABLE) {
      throw new CommitException(
          ErrorCode.RESOURCE_EXHAUSTED,
          "Max number of commits per table reached = " + MAX_NUM_COMMITS_PER_TABLE);
    }
    COMMIT_REPOSITORY.saveCommit(session, commit);
    if (latestBackfilledVersion >= firstCommit.getCommitVersion()) {
      COMMIT_REPOSITORY.backfillCommits(
          session,
          UUID.fromString(commit.getTableId()),
          latestBackfilledVersion,
          firstCommit,
          commit.getCommitInfo().getVersion());
    }
  }

  private static Long getLatestBackfilledVersion(
      Commit commit, CommitDAO firstCommit, CommitDAO lastCommit) {
    long latestBackfilledVersion;
    if (lastCommit.getIsBackfilledLatestCommit()) {
      if (!Objects.equals(firstCommit.getCommitVersion(), lastCommit.getCommitVersion())) {
        LOGGER.error(
            "When the last commit is the latest backfilled commit, there should be exactly one commit");
        // This is recoverable
      }
      latestBackfilledVersion = lastCommit.getCommitVersion();
    } else {
      latestBackfilledVersion =
          Math.max(
              commit.getLatestBackfilledVersion() == null ? 0 : commit.getLatestBackfilledVersion(),
              firstCommit.getCommitVersion() - 1);
    }
    return latestBackfilledVersion;
  }
}
