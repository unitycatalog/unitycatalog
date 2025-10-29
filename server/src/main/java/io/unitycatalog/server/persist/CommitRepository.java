package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommitRepository.class);

  // The maximum number of commits per table.
  public static final Integer MAX_NUM_COMMITS_PER_TABLE = 50;
  // The "LIMIT" of each query. It's larger so that queries can handle the commits over the limit.
  public static final Integer NUM_COMMITS_PER_BATCH = 100;

  private final SessionFactory sessionFactory;

  public CommitRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * @param tableId the table id to get the latest commits for
   * @return the latest commits for the table sorted by commit version in descending order up to
   *     NUM_COMMITS_PER_BATCH
   */
  private List<CommitDAO> getAllCommits(UUID tableId) {
    LOGGER.debug("Getting all commits of table id: {}", tableId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          // yili: check table setting
          Query<CommitDAO> query =
              session.createQuery(
                  "FROM CommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
                  CommitDAO.class);
          query.setParameter("tableId", tableId);
          query.setMaxResults(NUM_COMMITS_PER_BATCH);
          return query.list();
        },
        "Failed to get latest commits",
        /* readOnly = */ true);
  }

  public GetCommitsResponse getCommits(UUID tableId, long startVersion, Optional<Long> endVersion) {
    ValidationUtils.validateNonNull(tableId, "table_id");
    ValidationUtils.validateLongFieldNonNegative(startVersion, "start_version");
    if (endVersion.isPresent() && endVersion.get() < startVersion) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "end_version must be >=start_version if set");
    }

    // TODO(yili): check delta.feature.catalogOwned-preview
    List<CommitDAO> allCommits = getAllCommits(tableId);
    int commitCount = allCommits.size();
    if (commitCount > MAX_NUM_COMMITS_PER_TABLE) {
      // This should never occur.
      LOGGER.error(
          "Table {} has {} commits, which exceeds the limit of {}.",
          tableId,
          commitCount,
          MAX_NUM_COMMITS_PER_TABLE);
    }
    if (commitCount == 0) {
      // No commit exist yet
      return new GetCommitsResponse().latestTableVersion(-1L);
    }
    // Descending order
    CommitDAO firstCommit = allCommits.get(allCommits.size() - 1);
    CommitDAO lastCommit = allCommits.get(0);
    assert firstCommit.getCommitVersion() <= lastCommit.getCommitVersion();
    if (lastCommit.getIsBackfilledLatestCommit()) {
      // The newest commit is already backfilled. Just return an empty list. No need to return any
      // actual commits.
      return new GetCommitsResponse().latestTableVersion(lastCommit.getCommitVersion());
    }

    long paginatedEndVersionInclusive =
        Math.max(startVersion, firstCommit.getCommitVersion()) + MAX_NUM_COMMITS_PER_TABLE - 1;
    long effectiveEndVersionInclusive =
        Math.min(endVersion.orElse(Long.MAX_VALUE), paginatedEndVersionInclusive);
    List<CommitInfo> commits =
        allCommits.stream()
            .filter(
                c ->
                    c.getCommitVersion() >= startVersion
                        && c.getCommitVersion() <= effectiveEndVersionInclusive)
            .map(CommitDAO::toCommitInfo)
            .collect(Collectors.toList());
    return new GetCommitsResponse()
        .commits(commits)
        .latestTableVersion(lastCommit.getCommitVersion());
  }

  public void postCommit(Commit commit) {
    validateCommit(commit);
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID tableId = UUID.fromString(commit.getTableId());
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          validateTable(commit, tableInfoDAO);
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
            } else if (lastCommit.getIsDisownCommit()) {
              // This should never happen since disown commits are not allowed
              throw new BaseException(
                  ErrorCode.UNIMPLEMENTED, "Reboarding commits are not supported!");
            } else {
              handleNormalCommit(session, tableId, tableInfoDAO, commit, firstCommit, lastCommit);
            }
          }
          return null;
        },
        "Error commiting to table: " + commit.getTableId(),
        /* readOnly = */ false);
  }

  private static void handleOnboardingCommit(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, Commit commit) {
    CommitInfo commitInfo = commit.getCommitInfo();
    ValidationUtils.validateNonNull(
        commitInfo, Commit.JSON_PROPERTY_COMMIT_INFO + " in onboarding commit");
    if (Boolean.TRUE.equals(commitInfo.getIsDisownCommit())) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Onboarding commit cannot be a disown commit");
    }
    saveCommit(session, tableId, commitInfo);
    updateTableMetadata(session, tableId, tableInfoDAO, commit.getMetadata());
  }

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
              "Should not backfill version %d while the last version commited is %d",
              latestBackfilledVersion, lastCommitVersion));
    }
    backfillCommits(
        session, tableId, latestBackfilledVersion, firstCommitVersion, lastCommitVersion);
  }

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
              "Commit version must be the next version after the latest commit %d," + " but got %d",
              lastCommitVersion, newCommitVersion));
    }
    // getLatestBackfilledVersion may or may not be null because normal commit may or may
    // not notify a backfill in the same request
    @Nullable Long latestBackfilledVersion = commit.getLatestBackfilledVersion();
    if (latestBackfilledVersion != null && latestBackfilledVersion > lastCommitVersion) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Latest backfilled version cannot be greater than the last commit version = "
              + lastCommitVersion);
    }
    long effectiveBackfilledVersion =
        getEffectiveBackfilledVersion(tableId, latestBackfilledVersion, firstCommit, lastCommit);
    checkCommitLimit(tableId, effectiveBackfilledVersion, newCommitVersion);
    saveCommit(session, tableId, commitInfo);
    updateTableMetadata(session, tableId, tableInfoDAO, commit.getMetadata());
    if (latestBackfilledVersion != null) {
      backfillCommits(
          session, tableId, latestBackfilledVersion, firstCommitVersion, newCommitVersion);
    }
  }

  private static long getEffectiveBackfilledVersion(
      UUID tableId,
      @Nullable Long latestBackfilledVersion,
      CommitDAO firstCommit,
      CommitDAO lastCommit) {
    if (lastCommit.getIsBackfilledLatestCommit()) {
      if (!Objects.equals(firstCommit.getCommitVersion(), lastCommit.getCommitVersion())) {
        // This means a bug in this implementation, but recoverable.
        LOGGER.error(
            "Table: {}. Latest commit is marked backfilled but there are {} commits.",
            tableId,
            lastCommit.getCommitVersion() - firstCommit.getCommitVersion() + 1);
        // This is recoverable
      }
      return lastCommit.getCommitVersion();
    } else if (latestBackfilledVersion != null
        && latestBackfilledVersion >= firstCommit.getCommitVersion()) {
      return latestBackfilledVersion;
    } else {
      return firstCommit.getCommitVersion() - 1L;
    }
  }

  private static void checkCommitLimit(
      UUID tableId, long effectiveBackfilledVersion, long newCommitVersion) {
    long expectedFirstCommitAfterBackfill = effectiveBackfilledVersion + 1L;
    long expectedCommitCountPostCommit = newCommitVersion - expectedFirstCommitAfterBackfill + 1L;
    if (expectedCommitCountPostCommit > MAX_NUM_COMMITS_PER_TABLE) {
      throw new BaseException(
          ErrorCode.RESOURCE_EXHAUSTED, "Max number of commits per table reached: " + tableId);
    }
  }

  private static void backfillCommits(
      Session session,
      UUID tableId,
      long latestBackfilledVersion,
      long firstCommitVersion,
      long highestCommitVersion) {
    assert latestBackfilledVersion <= highestCommitVersion;
    // In all cases, keep at least the highest commit itself.
    long deleteUpTo = Math.min(latestBackfilledVersion, highestCommitVersion - 1L);
    if (latestBackfilledVersion > deleteUpTo) {
      // If the latest backfilled version won't be deleted (because that's the highest commit),
      // mark it as backfilled instead.
      markCommitAsLatestBackfilled(session, tableId, latestBackfilledVersion);
    }

    long numCommitsToDelete = deleteUpTo - firstCommitVersion + 1L;
    if (numCommitsToDelete <= 0) {
      return;
    }

    // Retry backfilling 5 times to prioritize cleaning of the commit table and log bugs where there
    // are more commits in the table than MAX_NUM_COMMITS_PER_TABLE
    int MAX_ITERATIONS = 5;
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

  private static int deleteCommitsUpTo(Session session, UUID tableId, long upToCommitVersion) {
    NativeQuery<CommitDAO> query =
        session.createNativeQuery(
            "DELETE FROM uc_commits WHERE table_id = :tableId AND commit_version <= :upToCommitVersion LIMIT :numCommitsPerBatch",
            CommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setParameter("upToCommitVersion", upToCommitVersion);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  private static int deleteCommits(Session session, UUID tableId) {
    NativeQuery<CommitDAO> query =
        session.createNativeQuery(
            "DELETE FROM uc_commits WHERE table_id = :tableId LIMIT :numCommitsPerBatch",
            CommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  private static void markCommitAsLatestBackfilled(
      Session session, UUID tableId, long commitVersion) {
    NativeQuery<CommitDAO> query =
        session.createNativeQuery(
            "UPDATE uc_commits SET is_backfilled_latest_commit = true WHERE table_id = :tableId "
                + "AND commit_version = :commitVersion",
            CommitDAO.class);
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

  private static void updateTableMetadata(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, @Nullable Metadata metadata) {
    if (metadata == null) {
      return;
    }
    if (metadata.getProperties() != null) {
      // Update properties
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
    ValidationUtils.validateNonEmpty(commit.getTableId(), Commit.JSON_PROPERTY_TABLE_ID);
    ValidationUtils.validateNonEmpty(commit.getTableUri(), Commit.JSON_PROPERTY_TABLE_URI);

    // Validate the commit info object
    if (commit.getCommitInfo() != null) {
      CommitInfo commitInfo = commit.getCommitInfo();
      // We do not support disown commits
      if (Boolean.TRUE.equals(commitInfo.getIsDisownCommit())) {
        throw new BaseException(ErrorCode.UNIMPLEMENTED, "Disown commits are not supported!");
      }
      ValidationUtils.validateLongFieldPositive(
          commitInfo.getVersion(), CommitInfo.JSON_PROPERTY_VERSION);
      ValidationUtils.validateLongFieldPositive(
          commitInfo.getTimestamp(), CommitInfo.JSON_PROPERTY_TIMESTAMP);
      ValidationUtils.validateNonEmpty(
          commitInfo.getFileName(), CommitInfo.JSON_PROPERTY_FILE_NAME);
      ValidationUtils.validateLongFieldPositive(
          commitInfo.getFileSize(), CommitInfo.JSON_PROPERTY_FILE_SIZE);
      ValidationUtils.validateLongFieldPositive(
          commitInfo.getFileModificationTimestamp(),
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

  private static void validateTable(Commit commit, TableInfoDAO tableInfoDAO) {
    ValidationUtils.validateEquals(
        tableInfoDAO.getType(),
        TableType.MANAGED.toString(),
        "Only managed tables are supported for coordinated commits");
    ValidationUtils.validateEquals(
        tableInfoDAO.getDataSourceFormat(),
        DataSourceFormat.DELTA.toString(),
        "Only delta tables are supported for coordinated commits");
    if (tableInfoDAO.getUrl() == null) {
      throw new BaseException(
          ErrorCode.DATA_LOSS, "Managed table doesn't have a URI:" + tableInfoDAO.getId());
    }
    ValidationUtils.validateEquals(
        commit.getTableUri(),
        tableInfoDAO.getUrl(),
        "Table URI in commit does not match the table path");
    // TODO(yili): check table properties here for catalogOwned
  }

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
