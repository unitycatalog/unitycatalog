package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfos;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.CommitInfo;
import io.unitycatalog.server.model.CommitMetadataProperties;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.Metadata;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Repository for managing coordinated commits for managed Delta tables in Unity Catalog. */
public class CommitRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommitRepository.class);

  /**
   * The maximum number of commits allowed per table before backfilling is required. TODO: turn this
   * into a configurable server property.
   */
  private static final int MAX_NUM_COMMITS_PER_TABLE = 50;

  /**
   * The batch size limit for delete operations. Set larger than MAX_NUM_COMMITS_PER_TABLE to handle
   * cases where tables exceed the maximum allowed commits. TODO: turn this into a configurable
   * server property.
   */
  private static final int NUM_COMMITS_PER_BATCH = 100;

  private final SessionFactory sessionFactory;
  private final ServerProperties serverProperties;

  public CommitRepository(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  /** Commits a new version to a managed Delta table with coordinated commit semantics. */
  public void postCommit(Commit commit) {
    serverProperties.checkManagedTableEnabled();
    validateCommit(commit);
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID tableId = UUID.fromString(commit.getTableId());
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + commit.getTableId());
          }
          validateTableForCommit(commit, tableInfoDAO);
          List<CommitDAO> firstAndLastCommits = getFirstAndLastCommits(session, tableId);
          if (firstAndLastCommits.isEmpty()) {
            handleOnboardingCommit(session, tableId, tableInfoDAO, commit);
          } else {
            CommitDAO firstCommit = firstAndLastCommits.get(0);
            CommitDAO lastCommit = firstAndLastCommits.get(1);
            assert firstCommit.getCommitVersion() <= lastCommit.getCommitVersion();
            if (commit.getCommitInfo() == null) {
              // This is already checked in validateCommit()
              assert commit.getLatestBackfilledVersion() != null;
              handleBackfillOnlyCommit(
                  session,
                  tableId,
                  commit.getLatestBackfilledVersion(),
                  firstCommit.getCommitVersion(),
                  lastCommit.getCommitVersion());
            } else {
              handleNormalCommit(session, tableId, tableInfoDAO, commit, firstCommit, lastCommit);
            }
          }
          return null;
        },
        "Error committing to table: " + commit.getTableId(),
        /* readOnly = */ false);
  }

  /**
   * Handles the first commit (onboarding) for a table that has no existing commits. This saves the
   * initial commit and optionally updates table metadata.
   */
  private static void handleOnboardingCommit(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, Commit commit) {
    CommitInfo commitInfo = commit.getCommitInfo();
    ValidationUtils.checkArgument(
        commitInfo != null,
        "Field can not be null: %s in onboarding commit",
        Commit.JSON_PROPERTY_COMMIT_INFO);
    saveCommit(session, tableId, commitInfo);
    Optional.ofNullable(commit.getMetadata())
        .ifPresent(metadata -> updateTableMetadata(session, tableId, tableInfoDAO, metadata));
  }

  /**
   * Handles a commit request that only performs backfilling without adding a new commit. This
   * validates the backfill version and delegates to the backfill logic.
   */
  private static void handleBackfillOnlyCommit(
      Session session,
      UUID tableId,
      long latestBackfilledVersion,
      long firstCommitVersion,
      long lastCommitVersion) {
    if (latestBackfilledVersion > lastCommitVersion) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Should not backfill version %d while the last version committed is %d",
              latestBackfilledVersion, lastCommitVersion));
    }
    backfillCommits(
        session,
        tableId,
        latestBackfilledVersion,
        firstCommitVersion,
        lastCommitVersion,
        Optional.empty());
  }

  /**
   * Handles a normal commit request that adds a new commit version to the table. This method
   * validates the commit version, checks commit limits, saves the new commit, updates table
   * metadata if provided, and performs backfilling if requested.
   */
  private static void handleNormalCommit(
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      Commit commit,
      CommitDAO firstCommit,
      CommitDAO lastCommit) {
    CommitInfo commitInfo = Objects.requireNonNull(commit.getCommitInfo());
    long firstCommitVersion = firstCommit.getCommitVersion();
    long lastCommitVersion = lastCommit.getCommitVersion();
    long newCommitVersion = commitInfo.getVersion();
    if (newCommitVersion <= lastCommitVersion) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS,
          "Commit version already accepted. Current table version is " + lastCommitVersion);
    }
    if (newCommitVersion > lastCommitVersion + 1) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Commit version must be the next version after the latest commit %d, but got %d",
              lastCommitVersion, newCommitVersion));
    }
    // getLatestBackfilledVersion may or may not be null because normal commit may or may
    // not notify a backfill in the same request
    Optional<Long> latestBackfilledVersion =
        Optional.ofNullable(commit.getLatestBackfilledVersion());
    if (latestBackfilledVersion.filter(x -> x > lastCommitVersion).isPresent()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Latest backfilled version %d cannot be greater than the last commit version = %d",
              latestBackfilledVersion.get(), lastCommitVersion));
    }
    long effectiveBackfilledVersion =
        getEffectiveBackfilledVersion(tableId, latestBackfilledVersion, firstCommit, lastCommit);
    checkCommitLimit(tableId, effectiveBackfilledVersion, newCommitVersion);
    saveCommit(session, tableId, commitInfo);
    Optional.ofNullable(commit.getMetadata())
        .ifPresent(metadata -> updateTableMetadata(session, tableId, tableInfoDAO, metadata));
    latestBackfilledVersion.ifPresent(
        latestBackfilled ->
            backfillCommits(
                session,
                tableId,
                latestBackfilled,
                firstCommitVersion,
                lastCommitVersion,
                Optional.of(newCommitVersion)));
  }

  /**
   * Calculates the would-be backfilled version AFTER the current commit request is completed. This
   * considers the current state of commits and any backfill information in the request.
   *
   * <p>The logic handles several cases:
   *
   * <ul>
   *   <li>If the last commit is already marked as backfilled, it remains the backfilled version
   *   <li>If the request specifies a valid backfilled version >= first commit, use that
   *   <li>If the first commit is marked as backfilled, use its version
   *   <li>Otherwise, nothing is backfilled yet (returns firstCommitVersion - 1)
   * </ul>
   */
  private static long getEffectiveBackfilledVersion(
      UUID tableId,
      Optional<Long> latestBackfilledVersion,
      CommitDAO firstCommit,
      CommitDAO lastCommit) {
    if (lastCommit.getIsBackfilledLatestCommit()) {
      // There should only be ONE commit if the last one is marked as backfilled which is the
      // special case.
      if (!Objects.equals(firstCommit.getCommitVersion(), lastCommit.getCommitVersion())) {
        // This means a bug in this implementation, but recoverable.
        LOGGER.error(
            "Table: {}. Latest commit is marked backfilled but there are {} commits.",
            tableId,
            lastCommit.getCommitVersion() - firstCommit.getCommitVersion() + 1);
      }
      // In this case:
      // 1. If the request wants to commit a newer version, it cannot possibly backfill the same
      // newer version in the same request. So the last backfilled version remains the same.
      // 2. If by any means the request still wants to report backfilled version in this request,
      // it must be <= lastCommit. So it's the same result.
      return lastCommit.getCommitVersion();
    } else if (latestBackfilledVersion
        .filter(x -> x >= firstCommit.getCommitVersion())
        .isPresent()) {
      // The commit request reports a valid backfilled version. We'll take that.
      return latestBackfilledVersion.get();
    } else if (firstCommit.getIsBackfilledLatestCommit()) {
      // The firstCommit is already backfilled. It remains only because it was the only commit
      // left when being backfilled.
      return firstCommit.getCommitVersion();
    } else {
      // Otherwise, nothing in [first, last] is/will be backfilled.
      return firstCommit.getCommitVersion() - 1L;
    }
  }

  /**
   * Validates that adding the new commit will not exceed the maximum number of commits allowed per
   * table after the commit (and backfill if any) is finished.
   */
  private static void checkCommitLimit(
      UUID tableId, long effectiveBackfilledVersion, long newCommitVersion) {
    long expectedFirstCommitAfterBackfill = effectiveBackfilledVersion + 1L;
    long expectedCommitCountPostCommit = newCommitVersion - expectedFirstCommitAfterBackfill + 1L;
    if (expectedCommitCountPostCommit > MAX_NUM_COMMITS_PER_TABLE) {
      throw new BaseException(
          ErrorCode.RESOURCE_EXHAUSTED, "Max number of commits per table reached: " + tableId);
    }
  }

  /**
   * Performs the backfilling operation by deleting old commits up to the specified version. The
   * most recent commit is always preserved as it serves as an indicator of the current table
   * version.
   *
   * <p>For backfill-only requests (when newCommitVersion is empty), if the backfilled version
   * equals the last commit version, that commit is marked as backfilled rather than deleted.
   *
   * <p>This method performs deletions in batches and retries up to 5 times if not all commits are
   * deleted, logging errors for investigation.
   */
  private static void backfillCommits(
      Session session,
      UUID tableId,
      long latestBackfilledVersion,
      long firstCommitVersion,
      long lastCommitVersion,
      Optional<Long> newCommitVersion) {
    // These asserts are already validated before calling this function
    assert latestBackfilledVersion <= lastCommitVersion;
    assert newCommitVersion.isEmpty() || newCommitVersion.get() == lastCommitVersion + 1;

    if (latestBackfilledVersion < firstCommitVersion) {
      // Backfilling a version that is already backfilled is fine. But no-op.
      return;
    }

    long highestCommitVersion = newCommitVersion.orElse(lastCommitVersion);
    // The last commit version, be it a new one or existing one, is never deleted.
    // It serves as an indicator of the current version.
    long deleteUpTo = Math.min(latestBackfilledVersion, highestCommitVersion - 1L);

    if (newCommitVersion.isEmpty() && latestBackfilledVersion == lastCommitVersion) {
      // Backfill only request will never delete the last existing commit. Instead, we mark it as
      // backfilled.
      markCommitAsLatestBackfilled(session, tableId, lastCommitVersion);
    }
    long numCommitsToDelete = deleteUpTo - firstCommitVersion + 1L;
    if (numCommitsToDelete <= 0) {
      // Nothing to delete.
      return;
    }

    // Retry backfilling 5 times to prioritize cleaning of the commit table and log bugs where there
    // are more commits in the table than MAX_NUM_COMMITS_PER_TABLE
    final int MAX_ITERATIONS = 5;
    for (int i = 0; i < MAX_ITERATIONS && numCommitsToDelete > 0; i++) {
      numCommitsToDelete -= deleteCommitsUpTo(session, tableId, deleteUpTo);
      if (numCommitsToDelete > 0) {
        LOGGER.error(
            "Failed to backfill commits for tableId: {}, upTo: {}, in batch: {}, commits left: {}",
            tableId,
            deleteUpTo,
            i,
            numCommitsToDelete);
      }
    }
  }

  private static void saveCommit(Session session, UUID tableId, CommitInfo commitInfo) {
    CommitDAO commitDAO = CommitDAO.from(tableId, commitInfo);
    session.persist(commitDAO);
  }

  /** Deletes commits up to and including the specified version. */
  private static int deleteCommitsUpTo(Session session, UUID tableId, long upToCommitVersion) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "DELETE FROM uc_commits WHERE table_id = :tableId AND commit_version <= :upToCommitVersion LIMIT :numCommitsPerBatch");
    query.setParameter("tableId", tableId);
    query.setParameter("upToCommitVersion", upToCommitVersion);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  /** Deletes commits for the specified table. */
  private static int deleteCommits(Session session, UUID tableId) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "DELETE FROM uc_commits WHERE table_id = :tableId LIMIT :numCommitsPerBatch");
    query.setParameter("tableId", tableId);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  /**
   * Marks a specific commit as the latest backfilled commit. This is used when a backfill-only
   * request backfills up to and including the last existing commit, which must be preserved.
   */
  private static void markCommitAsLatestBackfilled(
      Session session, UUID tableId, long commitVersion) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "UPDATE uc_commits SET is_backfilled_latest_commit = true WHERE table_id = :tableId "
                + "AND commit_version = :commitVersion");
    query.setParameter("tableId", tableId);
    query.setParameter("commitVersion", commitVersion);
    query.executeUpdate();
  }

  private List<CommitDAO> getFirstAndLastCommits(Session session, UUID tableId) {
    // Use native SQL to get the first and last commits since HQL doesn't support UNION ALL
    String sql =
        "(SELECT * FROM uc_commits WHERE table_id = :tableId "
            + "ORDER BY commit_version ASC LIMIT 1) "
            + "UNION ALL "
            + "(SELECT * FROM uc_commits WHERE table_id = :tableId "
            + "ORDER BY commit_version DESC LIMIT 1)";
    Query<CommitDAO> query = session.createNativeQuery(sql, CommitDAO.class);
    query.setParameter("tableId", tableId);
    List<CommitDAO> result = query.getResultList();
    // Sort to ensure the first commit is at index 0
    result.sort(Comparator.comparing(CommitDAO::getCommitVersion));
    return result;
  }

  /**
   * Updates table metadata including properties, schema (columns), and description based on the
   * metadata provided in a commit. This method handles:
   *
   * <ul>
   *   <li>Properties: Replaces all existing properties with new ones
   *   <li>Schema: Replaces all existing columns with new ones
   *   <li>Description: Updates the table comment
   * </ul>
   *
   * <p>The table's updated_at and updated_by fields are also refreshed.
   */
  private static void updateTableMetadata(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, Metadata metadata) {
    if (metadata.getProperties() != null) {
      // Update properties. They aren't part of TableInfoDAO so they'll do a separate update.
      PropertyRepository.findProperties(session, tableId, Constants.TABLE).forEach(session::remove);
      session.flush();
      PropertyDAO.from(metadata.getProperties().getProperties(), tableId, Constants.TABLE)
          .forEach(session::persist);
    }

    if (metadata.getSchema() != null) {
      // Update columns - clear existing and add new to trigger orphan removal
      List<ColumnInfoDAO> newColumns = ColumnInfoDAO.fromList(metadata.getSchema().getColumns());
      tableInfoDAO.getColumns().clear();
      session.flush(); // Flush to ensure old columns are deleted before adding new ones
      newColumns.forEach(
          c -> {
            c.setId(UUID.randomUUID());
            c.setTable(tableInfoDAO);
          });
      tableInfoDAO.getColumns().addAll(newColumns);
    }

    if (metadata.getDescription() != null) {
      // Update comment
      tableInfoDAO.setComment(metadata.getDescription());
    }

    String callerId = IdentityUtils.findPrincipalEmailAddress();
    tableInfoDAO.setUpdatedBy(callerId);
    tableInfoDAO.setUpdatedAt(new Date());
    session.merge(tableInfoDAO);
  }

  private static void validateCommit(Commit commit) {
    // Validate the commit object
    ValidationUtils.checkArgument(
        commit.getTableId() != null && !commit.getTableId().isEmpty(),
        "Field can not be empty: %s",
        Commit.JSON_PROPERTY_TABLE_ID);
    ValidationUtils.checkArgument(
        commit.getTableUri() != null && !commit.getTableUri().isEmpty(),
        "Field can not be empty: %s",
        Commit.JSON_PROPERTY_TABLE_URI);

    // Validate the commit info object
    if (commit.getCommitInfo() != null) {
      CommitInfo commitInfo = commit.getCommitInfo();
      ValidationUtils.checkArgument(
          commitInfo.getVersion() != null && commitInfo.getVersion() > 0,
          "Field must be positive: %s",
          CommitInfo.JSON_PROPERTY_VERSION);
      ValidationUtils.checkArgument(
          commitInfo.getTimestamp() != null && commitInfo.getTimestamp() > 0,
          "Field must be positive: %s",
          CommitInfo.JSON_PROPERTY_TIMESTAMP);
      ValidationUtils.checkArgument(
          commitInfo.getFileName() != null && !commitInfo.getFileName().isEmpty(),
          "Field can not be empty: %s",
          CommitInfo.JSON_PROPERTY_FILE_NAME);
      ValidationUtils.checkArgument(
          commitInfo.getFileSize() != null && commitInfo.getFileSize() > 0,
          "Field must be positive: %s",
          CommitInfo.JSON_PROPERTY_FILE_SIZE);
      ValidationUtils.checkArgument(
          commitInfo.getFileModificationTimestamp() != null
              && commitInfo.getFileModificationTimestamp() > 0,
          "Field must be positive: %s",
          CommitInfo.JSON_PROPERTY_FILE_MODIFICATION_TIMESTAMP);
      if (commit.getMetadata() != null) {
        Metadata metadata = commit.getMetadata();
        Optional<Map<String, String>> propertiesOpt =
            Optional.ofNullable(metadata.getProperties())
                .map(CommitMetadataProperties::getProperties);
        boolean hasProperties = propertiesOpt.map(p -> !p.isEmpty()).orElse(false);
        boolean hasSchema =
            Optional.ofNullable(metadata.getSchema())
                .map(ColumnInfos::getColumns)
                .map(c -> !c.isEmpty())
                .orElse(false);

        if (metadata.getDescription() == null && !hasProperties && !hasSchema) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "At least one of description, properties, or schema must be set in commit.metadata");
        }
        Optional<String> propertiesTableIdOpt = propertiesOpt.map(p -> p.get("ucTableId"));
        if (propertiesTableIdOpt.isEmpty()) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "commit does not contain ucTableId in the properties.");
        }
        if (!propertiesTableIdOpt.get().equals(commit.getTableId())) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              String.format(
                  "the table being committed (%s) does not match the properties ucTableId(%s).",
                  commit.getTableId(), propertiesTableIdOpt.get()));
        }
      }
    } else {
      // If commit info is null, then it should be a backfill only commit
      if (commit.getLatestBackfilledVersion() == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Either commit_info or latest_backfilled_version must be defined");
      }
      if (commit.getMetadata() != null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT, "metadata shouldn't be set for backfill only commit");
      }
    }
  }

  private static void validateTable(TableInfoDAO tableInfoDAO) {
    ValidationUtils.checkArgument(
        tableInfoDAO.getType() != null
            && tableInfoDAO.getType().equals(TableType.MANAGED.toString()),
        "Only managed tables are supported for coordinated commits");
    ValidationUtils.checkArgument(
        tableInfoDAO.getDataSourceFormat() != null
            && tableInfoDAO.getDataSourceFormat().equals(DataSourceFormat.DELTA.toString()),
        "Only delta tables are supported for coordinated commits");
    if (tableInfoDAO.getUrl() == null) {
      throw new BaseException(
          ErrorCode.DATA_LOSS, "Managed table doesn't have a URI: " + tableInfoDAO.getId());
    }
  }

  private static void validateTableForCommit(Commit commit, TableInfoDAO tableInfoDAO) {
    validateTable(tableInfoDAO);
    ValidationUtils.checkArgument(
        FileOperations.toStandardizedURIString(commit.getTableUri())
            .equals(FileOperations.toStandardizedURIString(tableInfoDAO.getUrl())),
        "Table URI in commit %s does not match the table path %s",
        commit.getTableUri(),
        tableInfoDAO.getUrl());
  }

  /**
   * Permanently deletes all commits associated with a table. This method is called when a table is
   * being deleted.
   */
  public void permanentlyDeleteTableCommits(Session session, UUID tableId) {
    // In case some tables got more commits than allowed, we still want to purge the commits
    // aggressively, so we allow 10x factor here. We also cap the number of iterations at 100 for
    // safety measures in case the constants are changed.
    int MAX_ITERATIONS = Math.min(100, MAX_NUM_COMMITS_PER_TABLE * 10 / NUM_COMMITS_PER_BATCH);
    boolean allDeleted = false;
    int numDeleted = 0;
    for (int i = 0; i < MAX_ITERATIONS; i++) {
      int deleted = deleteCommits(session, tableId);
      numDeleted += deleted;
      if (deleted < NUM_COMMITS_PER_BATCH) {
        allDeleted = true;
        break;
      }
    }
    if (numDeleted > MAX_NUM_COMMITS_PER_TABLE) {
      LOGGER.error(
          "Purged {} commits for table {}, which exceeds the maximum allowed number of "
              + "commits per table",
          numDeleted,
          tableId);
    }
    if (!allDeleted) {
      LOGGER.error(
          "Failed to purge all commits for table {} after {} iterations", tableId, MAX_ITERATIONS);
    }
  }
}
