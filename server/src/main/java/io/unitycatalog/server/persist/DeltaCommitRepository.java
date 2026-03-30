package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfos;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.DeltaCommitMetadataProperties;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.DeltaMetadata;
import io.unitycatalog.server.model.DeltaUniform;
import io.unitycatalog.server.model.DeltaUniformIceberg;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TableProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for managing Delta commits for managed Delta tables in Unity Catalog.
 *
 * <p>The database table 'uc_delta_commits' has commits of all managed tables until the commits are
 * backfilled. However, there's a special case that in order to record the last commit even after
 * it's backfilled, UC does not delete the last commit but instead only mark it as
 * is_backfilled_latest_commit=true. So it's guaranteed that there would be at least one record (the
 * last commit) once the table is onboarded.
 *
 * <p>For example, consider the following sequence of commit operations:
 *
 * <ol>
 *   <li>commit(v1). Then database has [v1]
 *   <li>commit(v2). Then database has [v1, v2]
 *   <li>commit(v3). Then database has [v1, v2, v3]
 *   <li>backfill(v1). Then database has [v2, v3]. Any version &lt;= v1 is removed.
 *   <li>commit(v4) and backfill(v3). Then database has [v4]. Any version &lt;= v3 is removed.
 *   <li>commit (v5). Then database has [v4, v5]
 *   <li>backfill(v5). Then database has [v5(is_backfilled_latest_commit=true)]. v4 is removed. But
 *       v5 has to be kept as the last record in database.
 *   <li>commit(v6). Then database has [v5(is_backfilled_latest_commit=true), v6]
 * </ol>
 */
public class DeltaCommitRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaCommitRepository.class);

  /**
   * The maximum number of unbackfilled commits allowed per table before backfilling is required. In
   * real life unbackfilled commits per table should remain 1 or 2 almost all the time as the client
   * should implement proactive backfilling right after committing a version. This limit exist as a
   * safety measure just in case there's a problem in client implementation, or the table is being
   * committed heavily by different clients in rare cases. TODO: turn this into a configurable
   * server property.
   */
  private static final int MAX_NUM_COMMITS_PER_TABLE = 10;

  /**
   * The batch size limit for commit delete and select operations. This limit is set to be larger
   * than MAX_NUM_COMMITS_PER_TABLE so that it should never hit this limit at all. But it serves the
   * purpose of another safety measure to avoid a huge query execution IF both the server and client
   * are not implemented correct and commits per table grow unbounded. TODO: turn this into a
   * configurable server property.
   */
  private static final int NUM_COMMITS_PER_BATCH = 20;

  /** The maximum size of the JSON that contains the Delta-to-Iceberg conversion information */
  public static final int MAX_DELTA_UNIFORM_ICEBERG_SIZE = 65535; // The limit from DAO

  public static final String ICEBERG_FORMAT = "iceberg";
  public static final String UNIFORM_ENABLED_FORMATS = "delta.universalFormat.enabledFormats";

  private final SessionFactory sessionFactory;
  private final ServerProperties serverProperties;

  public DeltaCommitRepository(SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
  }

  /**
   * Retrieves all commits for a table in descending order by version up to NUM_COMMITS_PER_BATCH.
   */
  private List<DeltaCommitDAO> getAllCommitDAOsDesc(UUID tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableId);
          }
          validateTable(tableInfoDAO);

          Query<DeltaCommitDAO> query =
              session.createQuery(
                  "FROM DeltaCommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
                  DeltaCommitDAO.class);
          query.setParameter("tableId", tableId);
          query.setMaxResults(NUM_COMMITS_PER_BATCH);
          return query.list();
        },
        "Failed to get latest commits",
        /* readOnly= */ true,
        // Use REPEATABLE_READ isolation to ensure consistent snapshot and prevent version numbers
        // from appearing to go backwards during concurrent writes. This is critical for get
        // commits that expect monotonic version progression after posting a commit.
        Optional.of(java.sql.Connection.TRANSACTION_REPEATABLE_READ));
  }

  /**
   * Retrieves commits for a managed Delta table within a specified version range.
   *
   * <p>This method returns commits that fall within the requested version range [startVersion,
   * endVersion]. The response includes both the list of commits and the latest table version.
   *
   * <p><b>Pagination:</b> Results may be further reduced to MAX_NUM_COMMITS_PER_TABLE commits per
   * request if they exceed. Earlier (lower version) commits in the version range of [startVersion,
   * endVersion] will be kept and later (higher version) commits in the version range will be
   * trimmed in order to keep the result within the limit.
   *
   * <p><b>Backfilled commits:</b> When commits are backfilled (persisted to file storage), they are
   * removed from the database. If the latest commit is marked as backfilled, this method returns an
   * empty commit list but still returns the correct latestTableVersion.
   *
   * <p><b>Empty table behavior:</b> If the table has no commits yet, returns latestTableVersion=0
   * with an empty commit list.
   *
   * <p>The returned commits are ordered by version in descending order (newest first).
   */
  public DeltaGetCommitsResponse getCommits(DeltaGetCommits rpc) {
    serverProperties.checkManagedTableEnabled();

    ValidationUtils.checkArgument(rpc.getTableId() != null, "Field can not be null: table_id");
    ValidationUtils.checkArgument(
        rpc.getStartVersion() != null, "Field can not be null: start_version");
    UUID tableId = UUID.fromString(rpc.getTableId());
    long startVersion = rpc.getStartVersion();
    Optional<Long> endVersion = Optional.ofNullable(rpc.getEndVersion());
    ValidationUtils.checkArgument(startVersion >= 0, "Field must be >=0: start_version");
    ValidationUtils.checkArgument(
        endVersion.filter(x -> x < startVersion).isEmpty(),
        "end_version must be >=start_version if set");

    List<DeltaCommitDAO> allCommitDAOsDesc = getAllCommitDAOsDesc(tableId);
    int commitCount = allCommitDAOsDesc.size();
    if (commitCount > MAX_NUM_COMMITS_PER_TABLE) {
      // This should never occur. But this is recoverable and not fatal.
      LOGGER.error(
          "Table {} has {} commits, which exceeds the limit of {}.",
          tableId,
          commitCount,
          MAX_NUM_COMMITS_PER_TABLE);
    }
    if (commitCount == 0) {
      // Table is validated as a managed Delta table. UC is the source of truth for
      // managed tables, so a newly created table with no commits is at version 0.
      return new DeltaGetCommitsResponse().latestTableVersion(0L);
    }

    // In case there's only one commit in database, firstCommitDAO and lastCommitDAO will be the
    // same object.
    DeltaCommitDAO firstCommitDAO = allCommitDAOsDesc.get(allCommitDAOsDesc.size() - 1);
    DeltaCommitDAO lastCommitDAO = allCommitDAOsDesc.get(0);
    assert firstCommitDAO.getCommitVersion() <= lastCommitDAO.getCommitVersion();
    if (lastCommitDAO.isBackfilledLatestCommit()) {
      // The last commit is already backfilled. Just return an empty list. No need to return any
      // actual commits.
      return new DeltaGetCommitsResponse().latestTableVersion(lastCommitDAO.getCommitVersion());
    }

    // The last version to return if endVersion is not set. It's limited by pagination limit.
    // In normal cases the pagination limitation should not happen at all since the limit is the
    // same limit that a table can have as many unbackfilled commits as possible. But it is
    // implemented this way just in case.
    long paginatedEndVersionInclusive =
        Math.max(startVersion, firstCommitDAO.getCommitVersion()) + MAX_NUM_COMMITS_PER_TABLE - 1;
    // The actual last version to return
    long effectiveEndVersionInclusive =
        Math.min(endVersion.orElse(Long.MAX_VALUE), paginatedEndVersionInclusive);

    // Filter result and return
    List<DeltaCommitInfo> commits =
        allCommitDAOsDesc.stream()
            .filter(
                c ->
                    !c.isBackfilledLatestCommit()
                        && c.getCommitVersion() >= startVersion
                        && c.getCommitVersion() <= effectiveEndVersionInclusive)
            .map(DeltaCommitDAO::toCommitInfo)
            .collect(Collectors.toList());
    return new DeltaGetCommitsResponse()
        .commits(commits)
        .latestTableVersion(lastCommitDAO.getCommitVersion());
  }

  /**
   * Commits a new version to a managed Delta table with coordinated commit semantics.
   *
   * <p>This method handles three types of commit operations:
   *
   * <ul>
   *   <li><b>Onboarding commit:</b> The first commit sent to Unity Catalog for this table
   *   <li><b>Normal commit:</b> A new version commit with optional backfill notification
   *   <li><b>Backfill-only commit:</b> No new version, only reports backfilled versions
   * </ul>
   *
   * <p>The method validates the commit, ensures the table is a managed Delta table, and performs
   * the appropriate commit operation within a transaction.
   *
   * @param commit the commit request containing version info, metadata, and backfill information
   * @throws BaseException if the commit is invalid, table is not found, or commit limits are
   *     exceeded
   */
  public void postCommit(DeltaCommit commit) {
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
          validateTableForCommit(session, commit, tableInfoDAO);
          List<DeltaCommitDAO> firstAndLastCommits = getFirstAndLastCommits(session, tableId);
          if (firstAndLastCommits.isEmpty()) {
            handleOnboardingCommit(session, tableId, tableInfoDAO, commit);
          } else {
            DeltaCommitDAO firstCommitDAO = firstAndLastCommits.get(0);
            DeltaCommitDAO lastCommitDAO = firstAndLastCommits.get(1);
            assert firstCommitDAO.getCommitVersion() <= lastCommitDAO.getCommitVersion();
            if (commit.getCommitInfo() == null) {
              // This is already checked in validateCommit()
              assert commit.getLatestBackfilledVersion() != null;
              handleBackfillOnlyCommit(
                  session,
                  tableId,
                  commit.getLatestBackfilledVersion(),
                  firstCommitDAO.getCommitVersion(),
                  lastCommitDAO.getCommitVersion());
            } else {
              handleNormalCommit(
                  session, tableId, tableInfoDAO, commit, firstCommitDAO, lastCommitDAO);
            }
          }
          return null;
        },
        "Error committing to table: " + commit.getTableId(),
        /* readOnly = */ false);
  }

  /**
   * Handles an onboarding commit, which is the very first commit sent to Unity Catalog for a table.
   *
   * <p>An onboarding commit must include commit information (version, timestamp, file details) but
   * does not perform backfilling since there are no prior Unity Catalog-managed versions. This may
   * be the first commit since table creation, or the table may have had previous filesystem-only
   * commits. After this commit, Unity Catalog becomes the commit coordinator for the table.
   *
   * <p>The method saves the commit and optionally updates table metadata if provided.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table being committed to
   * @param tableInfoDAO the table information data access object
   * @param commit the commit request containing version info and optional metadata
   * @throws BaseException if the commit info is null
   */
  private static void handleOnboardingCommit(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, DeltaCommit commit) {
    DeltaCommitInfo commitInfo = commit.getCommitInfo();
    ValidationUtils.checkArgument(
        commitInfo != null,
        "Field can not be null: %s in onboarding commit",
        DeltaCommit.JSON_PROPERTY_COMMIT_INFO);
    saveCommit(session, tableId, commitInfo);
    updateTableFromCommit(session, tableId, tableInfoDAO, commit);
  }

  /**
   * Handles a commit request that only performs backfilling without adding a new commit version.
   *
   * <p>This method is called when a commit request has no commit info but specifies a backfilled
   * version. It validates that the backfilled version is not greater than the last committed
   * version, then delegates to the backfill logic to remove old commits from the repository.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param latestBackfilledVersion the version up to which backfilling has already been performed
   * @param firstCommitVersion the version number of the first commit currently in the database
   * @param lastCommitVersion the version number of the last commit currently in the database
   * @throws BaseException if the backfilled version is greater than the last commit version
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
   * Handles a normal commit operation that adds a new version to the table.
   *
   * <p>A normal commit is any commit after the initial onboarding commit. It must include commit
   * info and may optionally:
   *
   * <ul>
   *   <li>Report backfilled versions to trigger cleanup of old commits
   *   <li>Update table metadata (schema, properties, description)
   * </ul>
   *
   * <p>The method validates that:
   *
   * <ul>
   *   <li>The new version is greater than the current version
   *   <li>The new version is exactly the next version (no gaps)
   *   <li>The backfilled version (if provided) is valid
   *   <li>Adding the new commit won't exceed the maximum commits per table limit
   * </ul>
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param tableInfoDAO the table information data access object
   * @param commit the commit request containing version info, optional backfill, and metadata
   * @param firstCommitDAO the first commit already in the database
   * @param lastCommitDAO the last commit already in the database
   * @throws BaseException if the commit version is invalid, already exists, or violates constraints
   */
  private static void handleNormalCommit(
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      DeltaCommit commit,
      DeltaCommitDAO firstCommitDAO,
      DeltaCommitDAO lastCommitDAO) {
    DeltaCommitInfo commitInfo = Objects.requireNonNull(commit.getCommitInfo());
    long firstCommitVersion = firstCommitDAO.getCommitVersion();
    long lastCommitVersion = lastCommitDAO.getCommitVersion();
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
    checkCommitLimit(
        tableId, newCommitVersion, latestBackfilledVersion, firstCommitDAO, lastCommitDAO);
    saveCommit(session, tableId, commitInfo);
    updateTableFromCommit(session, tableId, tableInfoDAO, commit);
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
   *
   * @param tableId the unique identifier of the table (used for logging)
   * @param latestBackfilledVersion optional backfilled version specified in the commit request
   * @param firstCommitDAO the first commit currently in the database with the lowest version
   * @param lastCommitDAO the last commit currently in the database with the highest version
   * @return the effective backfilled version after the commit operation completes
   */
  private static long getEffectiveBackfilledVersion(
      UUID tableId,
      Optional<Long> latestBackfilledVersion,
      DeltaCommitDAO firstCommitDAO,
      DeltaCommitDAO lastCommitDAO) {
    if (lastCommitDAO.isBackfilledLatestCommit()) {
      // There should only be ONE commit if the last one is marked as backfilled which is the
      // special case.
      if (firstCommitDAO.getCommitVersion() != lastCommitDAO.getCommitVersion()) {
        // This means a bug in this implementation, but recoverable.
        LOGGER.error(
            "Table: {}. Latest commit is marked backfilled but there are {} commits.",
            tableId,
            lastCommitDAO.getCommitVersion() - firstCommitDAO.getCommitVersion() + 1);
      }
      // In this case:
      // 1. If the request wants to commit a newer version, it cannot possibly backfill the same
      // newer version in the same request. So the last backfilled version remains the same.
      // 2. If by any means the request still wants to report backfilled version in this request,
      // it must be <= lastCommit. So it's the same result.
      return lastCommitDAO.getCommitVersion();
    } else if (latestBackfilledVersion
        .filter(x -> x >= firstCommitDAO.getCommitVersion())
        .isPresent()) {
      // The commit request reports a valid backfilled version. We'll take that.
      return latestBackfilledVersion.get();
    } else if (firstCommitDAO.isBackfilledLatestCommit()) {
      // The firstCommit is already backfilled. It remains only because it was the only commit
      // left when being backfilled.
      return firstCommitDAO.getCommitVersion();
    } else {
      // Otherwise, nothing in [first, last] is/will be backfilled.
      return firstCommitDAO.getCommitVersion() - 1L;
    }
  }

  /**
   * Validates that adding the new commit will not exceed the maximum number of commits allowed per
   * table after the commit (and backfill if any) is finished.
   *
   * @param tableId the unique identifier of the table
   * @param newCommitVersion the version number of the new commit being added
   * @param latestBackfilledVersion optional backfilled version specified in the commit request
   * @param firstCommitDAO the first commit currently in the database with the lowest version
   * @param lastCommitDAO the last commit currently in the database with the highest version
   * @throws BaseException if the commit would exceed the maximum commits per table limit
   */
  private static void checkCommitLimit(
      UUID tableId,
      long newCommitVersion,
      Optional<Long> latestBackfilledVersion,
      DeltaCommitDAO firstCommitDAO,
      DeltaCommitDAO lastCommitDAO) {
    long effectiveBackfilledVersion =
        getEffectiveBackfilledVersion(
            tableId, latestBackfilledVersion, firstCommitDAO, lastCommitDAO);
    long expectedFirstCommitVersionAfterBackfill = effectiveBackfilledVersion + 1L;
    long expectedCommitCountPostCommit =
        newCommitVersion - expectedFirstCommitVersionAfterBackfill + 1L;
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
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param latestBackfilledVersion the version up to which backfilling should be performed
   * @param firstCommitVersion the version number of the first commit currently in the database with
   *     the lowest version number
   * @param lastCommitVersion the version number of the last commit currently in the database with
   *     the highest version number
   * @param newCommitVersion optional new commit version being added (empty for backfill-only
   *     requests)
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

  /**
   * Persists a new commit record to the database.
   *
   * <p>Converts the commit info into a data access object and saves it to the commit repository.
   * This record includes the version number, timestamp, and file metadata for the commit.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param commitInfo the commit information containing version, timestamp, and file details
   */
  private static void saveCommit(Session session, UUID tableId, DeltaCommitInfo commitInfo) {
    DeltaCommitDAO deltaCommitDAO = DeltaCommitDAO.from(tableId, commitInfo);
    session.persist(deltaCommitDAO);
  }

  /**
   * Deletes commits up to and including the specified version.
   *
   * <p>This method executes a batch delete operation limited by {@code NUM_COMMITS_PER_BATCH}. If
   * more commits need to be deleted than the batch size, this method should be called multiple
   * times.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param upToCommitVersion the version number up to which commits should be deleted (inclusive)
   * @return the number of commits actually deleted in this batch
   */
  private static int deleteCommitsUpTo(Session session, UUID tableId, long upToCommitVersion) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "DELETE FROM uc_delta_commits WHERE table_id = :tableId AND commit_version <= :upToCommitVersion LIMIT :numCommitsPerBatch");
    query.setParameter("tableId", tableId);
    query.setParameter("upToCommitVersion", upToCommitVersion);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  /**
   * Deletes commits for the specified table in a single batch.
   *
   * <p>Unlike {@link #deleteCommitsUpTo(Session, UUID, long)}, this method deletes any commits for
   * the table without version filtering. The operation is limited by {@code NUM_COMMITS_PER_BATCH}.
   * Used primarily during table deletion to purge all commit history.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @return the number of commits actually deleted in this batch
   */
  private static int deleteCommits(Session session, UUID tableId) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "DELETE FROM uc_delta_commits WHERE table_id = :tableId LIMIT :numCommitsPerBatch");
    query.setParameter("tableId", tableId);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  /**
   * Marks a specific commit as the latest backfilled commit. This is used when a backfill-only
   * request backfills up to and including the last existing commit, which must be preserved.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param commitVersion the version number of the commit to mark as backfilled
   */
  private static void markCommitAsLatestBackfilled(
      Session session, UUID tableId, long commitVersion) {
    NativeQuery<?> query =
        session.createNativeQuery(
            "UPDATE uc_delta_commits SET is_backfilled_latest_commit = true WHERE table_id = :tableId "
                + "AND commit_version = :commitVersion");
    query.setParameter("tableId", tableId);
    query.setParameter("commitVersion", commitVersion);
    query.executeUpdate();
  }

  /**
   * Retrieves the first and last commits in database for a table ordered by version number.
   *
   * <p>The first commit is the commit in database with the lowest version number.
   *
   * <p>The last commit is the commit in database with the highest version number.
   *
   * <p>They may or may not be marked as backfilled already. But this function never count any of
   * the commits that are deleted by backfillCommits() since they are no longer in database.
   *
   * <p>Uses a UNION ALL query to efficiently fetch both boundary commits in a single database
   * operation. The results are sorted to ensure consistent ordering.
   *
   * <p>Return value interpretation:
   *
   * <ul>
   *   <li>Empty list: no commits exist for this table
   *   <li>List with two identical commits: only one commit exists (returned twice for consistency)
   *   <li>List with two different commits: [firstCommit, lastCommit] by version number
   * </ul>
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @return a list containing the first and last commits, empty if no commits exist
   */
  private List<DeltaCommitDAO> getFirstAndLastCommits(Session session, UUID tableId) {
    // Use native SQL to get the first and last commits since HQL doesn't support UNION ALL.
    // UNION ALL makes sure TWO rows are returned as long as there's any commit, even if there's
    // only one commit in table.
    String sql =
        "(SELECT * FROM uc_delta_commits WHERE table_id = :tableId "
            + "ORDER BY commit_version ASC LIMIT 1) "
            + "UNION ALL "
            + "(SELECT * FROM uc_delta_commits WHERE table_id = :tableId "
            + "ORDER BY commit_version DESC LIMIT 1)";
    Query<DeltaCommitDAO> query = session.createNativeQuery(sql, DeltaCommitDAO.class);
    query.setParameter("tableId", tableId);
    List<DeltaCommitDAO> result = query.getResultList();
    // Sort to ensure the first commit is at index 0
    result.sort(Comparator.comparing(DeltaCommitDAO::getCommitVersion));
    return result;
  }

  /**
   * Updates table with metadata and uniform information from a commit, then persists changes.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param tableInfoDAO the table information data access object to update
   * @param commit the commit request containing optional metadata and uniform information
   */
  private static void updateTableFromCommit(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, DeltaCommit commit) {
    boolean hasUpdates = false;

    if (commit.getMetadata() != null) {
      updateTableMetadata(session, tableId, tableInfoDAO, commit.getMetadata());
      hasUpdates = true;
    }

    if (commit.getUniform() != null) {
      updateTableUniform(tableInfoDAO, commit.getUniform());
      hasUpdates = true;
    }

    if (hasUpdates) {
      String callerId = IdentityUtils.findPrincipalEmailAddress();
      tableInfoDAO.setUpdatedBy(callerId);
      tableInfoDAO.setUpdatedAt(new Date());
      session.merge(tableInfoDAO);
    }
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
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param tableInfoDAO the table information data access object to update
   * @param metadata the metadata containing properties, schema, and/or description updates
   */
  private static void updateTableMetadata(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, DeltaMetadata metadata) {
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
  }

  /**
   * Updates table uniform metadata based on the uniform information provided in a commit.
   *
   * <p>This method updates the table's UniForm conversion metadata, including Iceberg metadata
   * location, converted Delta version, and conversion timestamp.
   *
   * @param tableInfoDAO the table information data access object to update
   * @param uniform the uniform metadata containing conversion information
   */
  private static void updateTableUniform(TableInfoDAO tableInfoDAO, DeltaUniform uniform) {
    DeltaUniformIceberg icebergMetadata = uniform.getIceberg();
    tableInfoDAO.setUniformIcebergMetadataLocation(
        NormalizedURL.normalize(icebergMetadata.getMetadataLocation().toString()));
    tableInfoDAO.setUniformIcebergConvertedDeltaVersion(icebergMetadata.getConvertedDeltaVersion());
    tableInfoDAO.setUniformIcebergConvertedDeltaTimestamp(
        Date.from(java.time.Instant.parse(icebergMetadata.getConvertedDeltaTimestamp())));
  }

  /**
   * Validates the structure and content of a commit request.
   *
   * <p>This method performs comprehensive validation including:
   *
   * <ul>
   *   <li>Table ID and URI must be non-null and non-empty
   *   <li>If commit info is present: validates version, timestamp, file name, file size, and file
   *       modification timestamp are positive/non-empty
   *   <li>If metadata is present: ensures at least one of description, properties, or schema is set
   *   <li>If metadata properties are present: validates that table ID property matches the commit's
   *       table ID
   *   <li>If commit info is absent: ensures this is a valid backfill-only commit with backfilled
   *       version set
   *   <li>If uniform is present: validates required fields and size limits
   * </ul>
   *
   * @param commit the commit request to validate
   * @throws BaseException if any validation rule is violated
   */
  private static void validateCommit(DeltaCommit commit) {
    // Validate the commit object
    ValidationUtils.checkArgument(
        commit.getTableId() != null && !commit.getTableId().isEmpty(),
        "Field can not be empty: %s",
        DeltaCommit.JSON_PROPERTY_TABLE_ID);
    ValidationUtils.checkArgument(
        commit.getTableUri() != null && !commit.getTableUri().isEmpty(),
        "Field can not be empty: %s",
        DeltaCommit.JSON_PROPERTY_TABLE_URI);

    // Validate the commit info object
    if (commit.getCommitInfo() != null) {
      DeltaCommitInfo commitInfo = commit.getCommitInfo();
      ValidationUtils.checkArgument(
          commitInfo.getVersion() != null && commitInfo.getVersion() > 0,
          "Field must be positive: %s",
          DeltaCommitInfo.JSON_PROPERTY_VERSION);
      ValidationUtils.checkArgument(
          commitInfo.getTimestamp() != null && commitInfo.getTimestamp() > 0,
          "Field must be positive: %s",
          DeltaCommitInfo.JSON_PROPERTY_TIMESTAMP);
      ValidationUtils.checkArgument(
          commitInfo.getFileName() != null && !commitInfo.getFileName().isEmpty(),
          "Field can not be empty: %s",
          DeltaCommitInfo.JSON_PROPERTY_FILE_NAME);
      ValidationUtils.checkArgument(
          commitInfo.getFileSize() != null && commitInfo.getFileSize() > 0,
          "Field must be positive: %s",
          DeltaCommitInfo.JSON_PROPERTY_FILE_SIZE);
      ValidationUtils.checkArgument(
          commitInfo.getFileModificationTimestamp() != null
              && commitInfo.getFileModificationTimestamp() > 0,
          "Field must be positive: %s",
          DeltaCommitInfo.JSON_PROPERTY_FILE_MODIFICATION_TIMESTAMP);
      if (commit.getMetadata() != null) {
        DeltaMetadata metadata = commit.getMetadata();
        Optional<Map<String, String>> propertiesOpt =
            Optional.ofNullable(metadata.getProperties())
                .map(DeltaCommitMetadataProperties::getProperties);
        boolean hasProperties = propertiesOpt.map(p -> !p.isEmpty()).orElse(false);
        boolean hasSchema =
            Optional.ofNullable(metadata.getSchema())
                .map(ColumnInfos::getColumns)
                .map(c -> !c.isEmpty())
                .orElse(false);

        if (metadata.getDescription() == null && !hasProperties && !hasSchema) {
          // metadata should only be set when there is an actual change in metadata.
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "At least one of description, properties, or schema must be set in commit.metadata");
        }
        if (propertiesOpt.isPresent()) {
          Optional<String> propertiesTableIdOpt =
              propertiesOpt.map(p -> p.get(TableProperties.UC_TABLE_ID_KEY));
          if (propertiesTableIdOpt.isEmpty()) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "commit does not contain %s in the properties.",
                    TableProperties.UC_TABLE_ID_KEY));
          }
          if (!propertiesTableIdOpt.get().equals(commit.getTableId())) {
            // This is to ensure that the Delta table's log on the file system has the table id
            // stored as a property. An extra check to ensure that the filesystem-based information
            // and the catalog-based information is in sync. for example, if some buggy connector
            // accidentally updated table A on the file system, but send the commit to table B in
            // UC, then this check will catch it as table A's properties in the log will have a
            // different id than the table B's id in UC.
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "the table being committed (%s) does not match the properties %s(%s).",
                    commit.getTableId(),
                    TableProperties.UC_TABLE_ID_KEY,
                    propertiesTableIdOpt.get()));
          }
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

    // Validate uniform metadata if present
    if (commit.getUniform() != null) {
      validateUniformIceberg(commit, commit.getUniform());
    }
  }

  /**
   * Validates Delta UniForm Iceberg metadata to ensure all required fields are present, the size is
   * within limits, and the converted delta version matches the commit version.
   *
   * @param commit the commit containing version information
   * @param uniform the uniform metadata to validate
   * @throws BaseException if validation fails
   */
  private static void validateUniformIceberg(DeltaCommit commit, DeltaUniform uniform) {
    if (uniform == null) {
      return; // DeltaUniform is optional
    }

    // If uniform is specified, iceberg must be set
    ValidationUtils.checkArgument(
        uniform.getIceberg() != null, "Field cannot be null in uniform: iceberg");

    DeltaUniformIceberg iceberg = uniform.getIceberg();

    // Validate that all required fields are present
    ValidationUtils.checkArgument(
        iceberg.getMetadataLocation() != null,
        "Field cannot be null in uniform.iceberg: metadata_location");
    ValidationUtils.checkArgument(
        iceberg.getConvertedDeltaVersion() != null,
        "Field cannot be null in uniform.iceberg: converted_delta_version");
    ValidationUtils.checkArgument(
        iceberg.getConvertedDeltaTimestamp() != null
            && !iceberg.getConvertedDeltaTimestamp().isEmpty(),
        "Field cannot be null or empty in uniform.iceberg: converted_delta_timestamp");

    // Validate that convertedDeltaVersion matches the commit version
    if (commit.getCommitInfo() != null) {
      ValidationUtils.checkArgument(
          iceberg.getConvertedDeltaVersion().equals(commit.getCommitInfo().getVersion()),
          "uniform.iceberg.converted_delta_version (%d) must equal commit version (%d)",
          iceberg.getConvertedDeltaVersion(),
          commit.getCommitInfo().getVersion());
    }

    // We check the size of the Delta-to-Iceberg conversion information here to fail early if
    // it exceeds the maximum size of DAO limit. The size is not accurate in terms of the size of
    // the object in the database but serves as a sanity check to ensure that we're not storing
    // excessively large objects.
    int size = iceberg.getMetadataLocation().toString().length();
    ValidationUtils.checkArgument(
        size <= MAX_DELTA_UNIFORM_ICEBERG_SIZE,
        "Delta UniForm Iceberg metadata size (%d bytes) exceeds maximum allowed size (%d bytes)",
        size,
        MAX_DELTA_UNIFORM_ICEBERG_SIZE);
  }

  /**
   * Validates that a table is eligible for Delta commits.
   *
   * <p>For a table to support Delta commits, it must:
   *
   * <ul>
   *   <li>Be a managed table (not external)
   *   <li>Use the Delta data source format
   *   <li>Have a valid URI/URL defined
   * </ul>
   *
   * @param tableInfoDAO the table information data access object to validate
   * @throws BaseException if the table doesn't meet requirements for Delta commits
   */
  private static void validateTable(TableInfoDAO tableInfoDAO) {
    ValidationUtils.checkArgument(
        tableInfoDAO.getType() != null
            && tableInfoDAO.getType().equals(TableType.MANAGED.toString()),
        "Only managed tables are supported for Delta commits");
    ValidationUtils.checkArgument(
        tableInfoDAO.getDataSourceFormat() != null
            && tableInfoDAO.getDataSourceFormat().equals(DataSourceFormat.DELTA.toString()),
        "Only delta tables are supported for Delta commits");
    if (tableInfoDAO.getUrl() == null) {
      throw new BaseException(
          ErrorCode.DATA_LOSS, "Managed table doesn't have a URI: " + tableInfoDAO.getId());
    }
  }

  /**
   * Validates that a table is eligible for the commit and that the commit's table URI matches.
   *
   * <p>This method first validates the table meets all requirements for Delta commits, then ensures
   * the table URI specified in the commit request matches the table's registered URI. URIs are
   * standardized before comparison to handle format variations.
   *
   * @param session the Hibernate session for database operations
   * @param commit the commit request containing the table URI
   * @param tableInfoDAO the table information data access object
   * @throws BaseException if validation fails or URIs don't match
   */
  private static void validateTableForCommit(
      Session session, DeltaCommit commit, TableInfoDAO tableInfoDAO) {
    validateTable(tableInfoDAO);
    NormalizedURL commitTableUri = NormalizedURL.from(commit.getTableUri());
    NormalizedURL tableUri = NormalizedURL.from(tableInfoDAO.getUrl());
    ValidationUtils.checkArgument(
        commitTableUri.equals(tableUri),
        "Table URI in commit %s does not match the table path %s",
        commit.getTableUri(),
        tableInfoDAO.getUrl());
    validateUniformMetadataPresence(session, commit, tableInfoDAO);
  }

  /**
   * Validates the presence of uniform metadata inside commit. If the table has UniForm enabled
   * after incoming commit, uniform metadata must exist inside commit Otherwise, if the table
   * doesn't have UniForm enabled after incoming commit, uniform metadata must not exist inside
   * commit
   *
   * @param session the Hibernate session for database operations
   * @param commit the commit request that may contain uniform metadata
   * @param tableInfoDAO the table information data access object
   * @throws BaseException if validation is violated
   */
  private static void validateUniformMetadataPresence(
      Session session, DeltaCommit commit, TableInfoDAO tableInfoDAO) {
    Map<String, String> effectiveProperties;
    // When properties are not null inside commit metadata, the incoming commit would update
    // table properties
    if (commit.getMetadata() != null && commit.getMetadata().getProperties() != null) {
      effectiveProperties =
          Optional.ofNullable(commit.getMetadata().getProperties().getProperties())
              .orElse(Collections.emptyMap());
    } else {
      // Incoming commit doesn't update table properties. Get current table properties from database
      List<PropertyDAO> properties =
          PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE);
      effectiveProperties = PropertyDAO.toMap(properties);
    }
    // Check if table has UniForm enabled after this commit
    boolean uniformEnabled =
        ICEBERG_FORMAT.equals(effectiveProperties.get(UNIFORM_ENABLED_FORMATS));
    if (uniformEnabled) {
      ValidationUtils.checkArgument(
          commit.getUniform() != null,
          "Uniform metadata must be set when table has UniForm enabled after the commit. "
              + "UniForm is enabled when property '%s'='%s'",
          UNIFORM_ENABLED_FORMATS,
          ICEBERG_FORMAT);
    } else {
      ValidationUtils.checkArgument(
          commit.getUniform() == null,
          "Uniform metadata must not be set when table has UniForm disabled after the commit. "
              + "UniForm is disabled when table does not have property '%s'='%s'",
          UNIFORM_ENABLED_FORMATS,
          ICEBERG_FORMAT);
    }
  }

  /**
   * Permanently deletes all commits associated with a table. This method is called when a table is
   * being deleted.
   *
   * <p>The method performs batch deletions with retries to handle tables that may have accumulated
   * more commits than the normal limit. It logs errors if the deletion exceeds expected thresholds
   * or fails to complete.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table whose commits should be deleted
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
