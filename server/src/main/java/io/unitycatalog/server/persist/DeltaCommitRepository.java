package io.unitycatalog.server.persist;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.TransactionRollbackException;
import io.unitycatalog.server.model.ColumnInfos;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaCommitInfo;
import io.unitycatalog.server.model.DeltaCommitMetadataProperties;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.DeltaMetadata;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.delta.DeltaUniformUtils;
import io.unitycatalog.server.service.delta.UcManagedDeltaContract;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for managing Delta commits for managed Delta tables in Unity Catalog.
 *
 * <p>How far a table has been backfilled is a single number on the table row, {@code
 * uc_tables.delta_latest_backfilled_version}. The <b>live commit window</b> -- what get-commits
 * returns, and what the per-table commit limit applies to -- is every row in {@code
 * uc_delta_commits} above that version. Backfill is therefore just an advance of that number; no
 * commit row is rewritten.
 *
 * <p>Rows at or below the backfilled version are kept for {@link #NUM_BACKFILLED_COMMITS_RETAINED}
 * more versions, purely so that a client retrying {@code add-commit} at an already-published
 * version can be answered from its file name: same name is a replay, a different name is a
 * conflict. Without them the server would have to compare the staged commit file against the
 * published one, and backfill is exactly what deletes the staged file -- which is how a retry ends
 * up stuck on {@code COMMIT_STATE_UNKNOWN}. A retry older than the retention window still falls
 * back to that content check.
 *
 * <p>For example, with a retention window of 2 versions:
 *
 * <ol>
 *   <li>commit(v1), commit(v2), commit(v3). Rows [v1, v2, v3], backfilled=0, live [v1, v2, v3]
 *   <li>backfill(v1). Rows unchanged, backfilled=1, live [v2, v3]
 *   <li>commit(v4) and backfill(v3). Rows unchanged, backfilled=3, live [v4]
 *   <li>commit(v5) and backfill(v4). Row v1 is now more than 2 versions below v4, so it is deleted.
 *       Rows [v2, v3, v4, v5], backfilled=4, live [v5]
 * </ol>
 *
 * <p>Tables written before {@code delta_latest_backfilled_version} existed have it null, and their
 * backfilled rows were deleted outright rather than retained. {@link
 * #deriveLatestBackfilledVersion} reconstructs the number from that older layout on first use.
 */
public class DeltaCommitRepository {

  /**
   * Thrown when a commit request is recognized as an idempotent replay of an already-accepted
   * commit, to roll the whole transaction back so nothing the request applied (the commit, and on
   * the Delta path any sibling metadata) is persisted; the commit entry point then catches it and
   * reports a no-op success.
   *
   * <p>Not a client-facing error, so it deliberately does not extend {@link BaseException}. As a
   * {@link TransactionRollbackException}, {@code TransactionManager} rolls back and rethrows it
   * as-is instead of wrapping it into an {@code INTERNAL} error; it is always caught within {@link
   * #postCommit} / {@link io.unitycatalog.server.persist.TableRepository#updateTableForDelta} and
   * never reaches the HTTP layer.
   */
  static class CommitAlreadyAcceptedException extends TransactionRollbackException {}

  /**
   * Thrown when the DB alone cannot decide whether a commit at an already-taken version is a replay
   * or a conflict, because the version fell out of the retained window (or predates it) so its
   * staged file name is no longer tracked. Rolls the transaction back (releasing the table lock) so
   * the entry point can settle it out of the transaction by comparing the incoming staged commit
   * file against the published {@code _delta_log/<version>.json} (see {@link
   * #verifyContentReplayOrThrowConflict}).
   *
   * <p>Like {@link CommitAlreadyAcceptedException} this is a {@link TransactionRollbackException},
   * not a client-facing error, and is always caught within {@link #postCommit} / {@link
   * io.unitycatalog.server.persist.TableRepository#updateTableForDelta}.
   */
  @AllArgsConstructor
  static class CommitContentCheckRequiredException extends TransactionRollbackException {
    private final NormalizedURL tableLocation;
    private final long version;
    private final String stagedFileName;
  }

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

  /**
   * How many versions below the latest backfilled one keep their commit row. A retained row only
   * has to outlive the window in which a client may retry that version -- Delta gives up after a
   * few minutes -- so this is generous by orders of magnitude while bounding {@code
   * uc_delta_commits} at roughly this many rows per table rather than one row per version ever
   * committed. TODO: turn this into a configurable server property.
   */
  private static final long NUM_BACKFILLED_COMMITS_RETAINED = 1000L;

  /**
   * Upper bound on delete batches, sized from the retention window (plus the live window) with a
   * 10x margin so a table at the retention cap is emptied in one call. Reused for the retention
   * prune, where overshoot is harmless because the next backfill prunes again.
   */
  private static final int MAX_DELETE_BATCHES =
      (int)
          ((NUM_BACKFILLED_COMMITS_RETAINED + MAX_NUM_COMMITS_PER_TABLE)
              * 10
              / NUM_COMMITS_PER_BATCH);

  /** Chunk size for the streamed commit-file content comparison in {@link #hasSameFileContent}. */
  private static final int CONTENT_COMPARE_BUFFER_BYTES = 8192;

  private final SessionFactory sessionFactory;
  private final ServerProperties serverProperties;
  private final FileOperations fileOperations;

  public DeltaCommitRepository(
      SessionFactory sessionFactory,
      ServerProperties serverProperties,
      FileOperations fileOperations) {
    this.sessionFactory = sessionFactory;
    this.serverProperties = serverProperties;
    this.fileOperations = fileOperations;
  }

  /**
   * Result of querying unbackfilled commits for a table.
   *
   * @param commits unbackfilled commits (descending version order, newest first)
   * @param latestTableVersion the latest commit version (0 if no commits)
   * @param oldestVersion the oldest unbackfilled version (used for pagination base); the latest
   *     version when nothing is unbackfilled
   */
  record CommitQueryResult(
      List<DeltaCommitDAO> commits, long latestTableVersion, long oldestVersion) {}

  /**
   * Query unbackfilled commits for a table within an existing session. Returns commits in
   * descending version order (newest first) and the latest table version.
   *
   * <p>Handles empty tables (returns version 0) and fully backfilled tables (returns empty list
   * with correct version).
   *
   * <p>Read-only: unlike the commit path this does not persist a derived backfilled version for
   * tables that predate the column, it just interprets the older layout in place.
   *
   * @param latestBackfilledVersion the table's recorded backfilled version, or empty on a table
   *     that predates the column
   */
  CommitQueryResult getUnbackfilledCommits(
      Session session, UUID tableId, Optional<Long> latestBackfilledVersion) {
    // Unfiltered and newest-first: the live window is always the top of the log, so the retained
    // backfilled rows below it can only ever be trimmed off the end of this page.
    Query<DeltaCommitDAO> query =
        session.createQuery(
            "FROM DeltaCommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
            DeltaCommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setMaxResults(NUM_COMMITS_PER_BATCH);
    List<DeltaCommitDAO> allDesc = query.list();

    if (allDesc.isEmpty()) {
      return new CommitQueryResult(List.of(), 0L, 0L);
    }

    long latestVersion = allDesc.get(0).getCommitVersion();
    long backfilledThrough =
        latestBackfilledVersion.orElseGet(() -> legacyBackfilledVersionOf(allDesc));
    List<DeltaCommitDAO> unbackfilled =
        allDesc.stream().filter(c -> c.getCommitVersion() > backfilledThrough).toList();

    if (unbackfilled.size() > MAX_NUM_COMMITS_PER_TABLE) {
      LOGGER.error(
          "Table {} has {} unbackfilled commits, exceeds limit {}.",
          tableId,
          unbackfilled.size(),
          MAX_NUM_COMMITS_PER_TABLE);
    }

    long oldestVersion =
        unbackfilled.isEmpty()
            ? latestVersion
            : unbackfilled.get(unbackfilled.size() - 1).getCommitVersion();
    return new CommitQueryResult(unbackfilled, latestVersion, oldestVersion);
  }

  /**
   * Reconstructs the backfilled version from a page of commit rows written before {@code
   * delta_latest_backfilled_version} existed. Back then backfilled rows were deleted rather than
   * retained, except the newest one, which was flagged so the table kept a record of its current
   * version. So everything still present above any flagged row is unbackfilled.
   *
   * @param allDesc a non-empty page of the table's commit rows, newest first
   */
  private static long legacyBackfilledVersionOf(List<DeltaCommitDAO> allDesc) {
    return allDesc.stream()
        .filter(DeltaCommitDAO::isBackfilledLatestCommit)
        .mapToLong(DeltaCommitDAO::getCommitVersion)
        .max()
        .orElseGet(() -> allDesc.get(allDesc.size() - 1).getCommitVersion() - 1L);
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
   * <p><b>Backfilled commits:</b> Commits at or below the table's backfilled version are omitted.
   * If everything is backfilled, this method returns an empty commit list but still returns the
   * correct latestTableVersion.
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

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          // Validate table exists and is managed Delta
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.TABLE_NOT_FOUND, "Table not found: " + tableId);
          }
          validateTable(tableInfoDAO);

          CommitQueryResult result =
              getUnbackfilledCommits(
                  session,
                  tableId,
                  Optional.ofNullable(tableInfoDAO.getDeltaLatestBackfilledVersion()));

          // Apply version range filter + pagination
          long paginatedEnd =
              Math.max(startVersion, result.oldestVersion()) + MAX_NUM_COMMITS_PER_TABLE - 1;
          long effectiveEnd = Math.min(endVersion.orElse(Long.MAX_VALUE), paginatedEnd);

          List<DeltaCommitInfo> commits =
              result.commits().stream()
                  .filter(
                      c ->
                          c.getCommitVersion() >= startVersion
                              && c.getCommitVersion() <= effectiveEnd)
                  .map(DeltaCommitDAO::toCommitInfo)
                  .collect(Collectors.toList());
          return new DeltaGetCommitsResponse()
              .commits(commits)
              .latestTableVersion(result.latestTableVersion());
        },
        "Failed to get commits",
        /* readOnly= */ true,
        Optional.of(TRANSACTION_REPEATABLE_READ));
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
   * <p>Replaying a commit already accepted for this table is an idempotent no-op success (not a
   * conflict), so a client that lost the response can safely resend. See {@link
   * #exceptionForAlreadyTakenVersion}.
   *
   * @param commit the commit request containing version info, metadata, and backfill information
   * @throws BaseException if the commit is invalid, table is not found, or commit limits are
   *     exceeded
   */
  public void postCommit(DeltaCommit commit) {
    try {
      postCommitInTransaction(commit);
    } catch (CommitAlreadyAcceptedException e) {
      // Idempotent replay: the transaction rolled back to a no-op. Report success.
    } catch (CommitContentCheckRequiredException e) {
      // Version was purged, so the DB couldn't decide: settle it out of the (rolled-back)
      // transaction by comparing file content. A match is a no-op success; a difference throws.
      verifyContentReplayOrThrowConflict(fileOperations, e);
    }
  }

  private void postCommitInTransaction(DeltaCommit commit) {
    serverProperties.checkManagedTableEnabled();
    validateCommit(commit);
    // Extract + shape-validate uniform fields outside the transaction. The subpath check (which
    // needs the table URL) happens later inside validateTableForCommit; everything else
    // uniform-related is settled here.
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields =
        DeltaUniformUtils.getUniformFields(commit);
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID tableId = UUID.fromString(commit.getTableId());
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO == null) {
            throw new BaseException(
                ErrorCode.TABLE_NOT_FOUND, "Table not found: " + commit.getTableId());
          }
          // Serialize all commit/backfill mutations on this table by write-locking its uc_tables
          // row, matching the Delta update path. This makes the commit-log reads and writes below
          // atomic against a concurrent commit or backfill.
          RepositoryUtils.lockTableForCommit(session, tableInfoDAO, tableId, Optional.empty());
          validateTableForCommit(session, commit, tableInfoDAO, uniformFields);
          postCommitCore(session, tableId, tableInfoDAO, commit, uniformFields);
          return null;
        },
        "Error committing to table: " + commit.getTableId(),
        /* readOnly= */ false);
  }

  /**
   * Shared core of {@link #postCommit} and {@link #applyCommitAndBackfillInSession}: read the
   * first/last commits and route to {@link #handleOnboardingCommit} (no prior commits + commit info
   * present), {@link #handleBackfillOnlyCommit} (prior commits + commit info absent), or {@link
   * #handleNormalCommit} (prior commits + commit info present). A backfill-only request against an
   * empty commit log is rejected here directly with a caller-aware message.
   *
   * <p>An idempotent replay is detected within {@link #handleNormalCommit}, which throws {@link
   * CommitAlreadyAcceptedException}.
   */
  private void postCommitCore(
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      DeltaCommit commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields) {
    Optional<DeltaCommitDAO> lastCommit = getLastCommit(session, tableId);
    if (lastCommit.isEmpty()) {
      if (commit.getCommitInfo() == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Backfill request requires a prior commit; the table's commit log is empty.");
      }
      handleOnboardingCommit(session, tableId, tableInfoDAO, commit, uniformFields);
    } else {
      long backfilledThrough = initializeLatestBackfilledVersion(session, tableInfoDAO);
      DeltaCommitDAO lastCommitDAO = lastCommit.get();
      ValidationUtils.checkArgument(
          backfilledThrough <= lastCommitDAO.getCommitVersion(),
          "Inconsistent commit log: backfilled version > last commit version.");
      if (commit.getCommitInfo() == null) {
        // latestBackfilledVersion non-null guaranteed upstream (UC REST: validateCommit;
        // Delta update: DeltaUpdateTableMapper checkNotNull).
        assert (commit.getLatestBackfilledVersion() != null);
        handleBackfillOnlyCommit(
            session,
            tableInfoDAO,
            fileOperations,
            commit.getLatestBackfilledVersion(),
            backfilledThrough,
            lastCommitDAO.getCommitVersion());
      } else {
        handleNormalCommit(
            session,
            tableId,
            tableInfoDAO,
            fileOperations,
            commit,
            uniformFields,
            backfilledThrough,
            lastCommitDAO);
      }
    }
  }

  /**
   * Returns the table's backfilled version, deriving and persisting it first if the table predates
   * the column. Must be called on the commit path only, with a non-empty commit log: it writes, and
   * it relies on the caller holding the table write lock.
   */
  private static long initializeLatestBackfilledVersion(Session session, TableInfoDAO dao) {
    Long recorded = dao.getDeltaLatestBackfilledVersion();
    if (recorded != null) {
      return recorded;
    }
    long derived = deriveLatestBackfilledVersion(session, dao.getId());
    setLatestBackfilledVersion(session, dao, derived);
    return derived;
  }

  /**
   * Reconstructs the backfilled version for a table written before {@code
   * delta_latest_backfilled_version} existed, where backfilled rows were deleted rather than
   * retained and only the newest was kept, flagged. See {@link #legacyBackfilledVersionOf} for the
   * same reasoning against an already-loaded page.
   *
   * <p>Runs at most once per table.
   */
  private static long deriveLatestBackfilledVersion(Session session, UUID tableId) {
    Long flagged =
        session
            .createQuery(
                "SELECT MAX(commitVersion) FROM DeltaCommitDAO"
                    + " WHERE tableId = :tableId AND isBackfilledLatestCommit = true",
                Long.class)
            .setParameter("tableId", tableId)
            .uniqueResult();
    if (flagged != null) {
      return flagged;
    }
    Long oldest =
        session
            .createQuery(
                "SELECT MIN(commitVersion) FROM DeltaCommitDAO WHERE tableId = :tableId",
                Long.class)
            .setParameter("tableId", tableId)
            .uniqueResult();
    // Nothing is backfilled, so the window starts at the oldest row. Versions below it were never
    // committed through Unity Catalog and must not be treated as awaiting backfill.
    return oldest - 1L;
  }

  private static void setLatestBackfilledVersion(Session session, TableInfoDAO dao, long version) {
    dao.setDeltaLatestBackfilledVersion(version);
    session.merge(dao);
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
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      DeltaCommit commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields) {
    DeltaCommitInfo commitInfo = commit.getCommitInfo();
    ValidationUtils.checkArgument(
        commitInfo != null,
        "Field can not be null: %s in onboarding commit",
        DeltaCommit.JSON_PROPERTY_COMMIT_INFO);
    saveCommit(session, tableId, commitInfo);
    // Unity Catalog becomes the commit coordinator at this version, so everything below it is
    // outside the commit log: not awaiting backfill, and never verified against storage.
    setLatestBackfilledVersion(session, tableInfoDAO, commitInfo.getVersion() - 1L);
    updateTableFromCommit(session, tableId, tableInfoDAO, commit, uniformFields);
  }

  /**
   * Handles a commit request that only performs backfilling without adding a new commit version.
   *
   * <p>This method is called when a commit request has no commit info but specifies a backfilled
   * version. It validates that the backfilled version is not greater than the last committed
   * version, then delegates to the backfill logic to advance the live commit window.
   *
   * @param session the Hibernate session for database operations
   * @param tableInfoDAO the table information data access object
   * @param fileOperations used to verify the published commit files before advancing
   * @param latestBackfilledVersion the version up to which backfilling has already been performed
   * @param backfilledThrough the table's currently recorded backfilled version
   * @param lastCommitVersion the version number of the last commit currently in the database
   * @throws BaseException if the backfilled version is greater than the last commit version
   */
  private static void handleBackfillOnlyCommit(
      Session session,
      TableInfoDAO tableInfoDAO,
      FileOperations fileOperations,
      long latestBackfilledVersion,
      long backfilledThrough,
      long lastCommitVersion) {
    if (latestBackfilledVersion > lastCommitVersion) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Should not backfill version %d while the last version committed is %d",
              latestBackfilledVersion, lastCommitVersion));
    }
    backfillCommits(
        session, tableInfoDAO, fileOperations, latestBackfilledVersion, backfilledThrough);
  }

  /**
   * Apply commit-log changes (add-commit and/or set-latest-backfilled-version) for the Delta
   * update-table endpoint, going through the shared {@link #postCommitCore} so the commit-log
   * progression is identical to {@link #postCommit}. Differs from {@code postCommit} on three
   * points:
   *
   * <ul>
   *   <li>does not open its own transaction -- the caller ({@link
   *       io.unitycatalog.server.persist.TableRepository}) holds it;
   *   <li>does not validate table-URI equality -- the Delta endpoint resolves the table by name and
   *       id;
   *   <li>does not project commit metadata onto the DAO -- the Delta path sends metadata changes as
   *       sibling {@code TableUpdate} actions which the mapper has already applied.
   * </ul>
   *
   * <p>The metadata-location subpath check on {@code uniformFields} runs here (the table location
   * comes from the DAO). Property/block-presence consistency is the responsibility of {@link
   * io.unitycatalog.server.service.delta.DeltaUpdateTableMapper#applyUpdates}, which runs against
   * the post-update {@link MutablePropertyMap} view; this method only sees the DAO.
   *
   * <p>Package-private because the only caller today is {@link
   * io.unitycatalog.server.persist.TableRepository}; widen the visibility once a second caller
   * needs it.
   *
   * @param session active Hibernate session owned by the caller's transaction.
   * @param dao the table to commit.
   * @param deltaCommitOpt the {@code add-commit} payload, if any; converted internally to the UC
   *     {@link DeltaCommitInfo} shape before dispatch.
   * @param uniformFields extracted + shape-validated UniForm-Iceberg fields from the same {@code
   *     add-commit}, if any; must be empty when {@code deltaCommitOpt} is empty.
   * @param latestBackfilledVersion the {@code set-latest-backfilled-version} target, if any. Same
   *     value as the Delta wire field {@code latest-published-version}. When paired with {@code
   *     deltaCommitOpt}, the helper runs both in one read of the commit log.
   */
  void applyCommitAndBackfillInSession(
      Session session,
      TableInfoDAO dao,
      Optional<DeltaCommitInfo> commitInfoOpt,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields,
      Optional<Long> latestBackfilledVersion) {
    ValidationUtils.checkArgument(
        commitInfoOpt.isPresent() || latestBackfilledVersion.isPresent(),
        "At least one of add-commit or set-latest-backfilled-version is required.");
    if (uniformFields.isPresent() && commitInfoOpt.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Uniform metadata requires an accompanying commit.");
    }
    serverProperties.checkManagedTableEnabled();
    uniformFields.ifPresent(
        uf ->
            DeltaUniformUtils.requireMetadataLocationSubpath(
                uf.metadataLocation(), NormalizedURL.from(dao.getUrl())));
    // Same per-field commit-info check as the UC REST validateCommit path.
    commitInfoOpt.ifPresent(DeltaCommitRepository::validateCommitInfo);
    postCommitCore(
        session,
        dao.getId(),
        dao,
        new DeltaCommit()
            .commitInfo(commitInfoOpt.orElse(null))
            .latestBackfilledVersion(latestBackfilledVersion.orElse(null)),
        uniformFields);
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
   * <p>A commit at an already-taken version is resolved by {@link
   * #exceptionForAlreadyTakenVersion}: a recognized replay throws {@link
   * CommitAlreadyAcceptedException}; a purged version that needs an out-of-transaction content
   * check throws {@link CommitContentCheckRequiredException}; anything else is a genuine conflict.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @param tableInfoDAO the table information data access object
   * @param fileOperations used to verify the published commit files before advancing
   * @param commit the commit request containing version info, optional backfill, and metadata
   * @param backfilledThrough the table's currently recorded backfilled version
   * @param lastCommitDAO the last commit already in the database
   * @throws CommitAlreadyAcceptedException if this is an idempotent replay of an accepted commit
   * @throws CommitContentCheckRequiredException if an unretained version needs a content check
   * @throws BaseException if the commit version is invalid, already exists, or violates constraints
   */
  private static void handleNormalCommit(
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      FileOperations fileOperations,
      DeltaCommit commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields,
      long backfilledThrough,
      DeltaCommitDAO lastCommitDAO) {
    DeltaCommitInfo commitInfo = Objects.requireNonNull(commit.getCommitInfo());
    long lastCommitVersion = lastCommitDAO.getCommitVersion();
    long newCommitVersion = commitInfo.getVersion();
    if (newCommitVersion <= lastCommitVersion) {
      // This version is already taken: throw the outcome (replay, content-check, or conflict).
      throw exceptionForAlreadyTakenVersion(
          session,
          tableInfoDAO,
          newCommitVersion,
          commitInfo.getFileName(),
          backfilledThrough,
          lastCommitDAO);
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
    checkCommitLimit(tableId, newCommitVersion, latestBackfilledVersion, backfilledThrough);
    saveCommit(session, tableId, commitInfo);
    updateTableFromCommit(session, tableId, tableInfoDAO, commit, uniformFields);
    latestBackfilledVersion.ifPresent(
        latestBackfilled ->
            backfillCommits(
                session, tableInfoDAO, fileOperations, latestBackfilled, backfilledThrough));
  }

  /**
   * Validates that adding the new commit will not exceed the maximum number of commits allowed per
   * table after the commit (and backfill if any) is finished.
   *
   * @param tableId the unique identifier of the table
   * @param newCommitVersion the version number of the new commit being added
   * @param latestBackfilledVersion optional backfilled version specified in the commit request
   * @param backfilledThrough the table's currently recorded backfilled version
   * @throws BaseException if the commit would exceed the maximum commits per table limit
   */
  private static void checkCommitLimit(
      UUID tableId,
      long newCommitVersion,
      Optional<Long> latestBackfilledVersion,
      long backfilledThrough) {
    // A request may only move the backfilled version forward, so the window after this request is
    // everything above whichever of the two is higher.
    long effectiveBackfilledVersion =
        Math.max(backfilledThrough, latestBackfilledVersion.orElse(backfilledThrough));
    if (newCommitVersion - effectiveBackfilledVersion > MAX_NUM_COMMITS_PER_TABLE) {
      throw new BaseException(
          ErrorCode.RESOURCE_EXHAUSTED, "Max number of commits per table reached: " + tableId);
    }
  }

  /**
   * Records that the table has been backfilled through {@code latestBackfilledVersion}, which
   * advances the live commit window past every version at or below it. No commit row is rewritten:
   * rows below the new watermark stay put until they age out of the retention window.
   *
   * <p>Before advancing, every newly-covered published {@code _delta_log/<version>.json} must
   * exist. Moving the watermark past a version whose published file is missing would drop that
   * version out of get-commits while nothing on storage replaces it, stranding readers.
   *
   * @param session the Hibernate session for database operations
   * @param tableInfoDAO the table whose watermark is advanced
   * @param fileOperations used to HEAD the published commit files before advancing
   * @param latestBackfilledVersion the version up to which backfilling should be performed
   * @param backfilledThrough the table's currently recorded backfilled version
   */
  private static void backfillCommits(
      Session session,
      TableInfoDAO tableInfoDAO,
      FileOperations fileOperations,
      long latestBackfilledVersion,
      long backfilledThrough) {
    if (latestBackfilledVersion <= backfilledThrough) {
      // Re-reporting a version that is already backfilled is fine, but a no-op.
      return;
    }

    // Only the versions this request newly covers are checked; earlier ones were checked when the
    // watermark passed them.
    requirePublishedCommitFiles(
        fileOperations,
        NormalizedURL.from(tableInfoDAO.getUrl()),
        backfilledThrough + 1L,
        latestBackfilledVersion);

    setLatestBackfilledVersion(session, tableInfoDAO, latestBackfilledVersion);
    pruneRetainedCommits(session, tableInfoDAO.getId(), latestBackfilledVersion);
  }

  /** Absolute path of the table's {@code _delta_log} directory. */
  static String deltaLogDir(NormalizedURL tableLocation) {
    return tableLocation + "/_delta_log";
  }

  /**
   * Absolute path of the published Delta commit file for {@code version} under {@code
   * tableLocation}. Locale.ROOT keeps the zero-padded name ASCII-digit regardless of server locale.
   */
  static String publishedCommitPath(NormalizedURL tableLocation, long version) {
    return String.format(Locale.ROOT, "%s/%020d.json", deltaLogDir(tableLocation), version);
  }

  /**
   * Absolute path of the staged commit file {@code fileName} under {@code
   * tableLocation/_delta_log/_staged_commits}.
   */
  static String stagedCommitPath(NormalizedURL tableLocation, String fileName) {
    return String.format(
        Locale.ROOT, "%s/_staged_commits/%s", deltaLogDir(tableLocation), fileName);
  }

  /**
   * HEADs each published {@code _delta_log/<version>.json} in [{@code fromVersion}, {@code
   * toVersion}]. No-op when {@code fromVersion > toVersion}.
   *
   * <p>A definitively absent file means the client reported a backfill that did not complete:
   * {@code INVALID_ARGUMENT} (400), since retrying cannot help until the file is published. FileIO
   * acquisition, HEAD, and close failures (including {@link BaseException}s from credential
   * vending) leave existence undetermined and must not be charged to the caller as a bad request:
   * {@code COMMIT_STATE_UNKNOWN} (500, retriable), matching {@link
   * #verifyContentReplayOrThrowConflict}. This matters most on a combined add-commit + backfill
   * request, where both share one transaction and a flaky HEAD would otherwise roll the new commit
   * back as a client error.
   */
  static void requirePublishedCommitFiles(
      FileOperations fileOperations,
      NormalizedURL tableLocation,
      long fromVersion,
      long toVersion) {
    if (fromVersion > toVersion) {
      return;
    }
    // Record a miss inside the FileIO block and throw INVALID_ARGUMENT after it, so credential
    // vending, HEAD, and close failures (which also throw BaseException) are not passed through
    // as a client 400. v == toVersion is an explicit stop so v++ cannot overflow at MAX_VALUE.
    fileOperations.validateReadAccessConfiguration(tableLocation);
    String missingPath = null;
    try (FileIO fileIO = fileOperations.getFileIO(tableLocation)) {
      for (long v = fromVersion; ; v++) {
        String path = publishedCommitPath(tableLocation, v);
        if (!fileIO.newInputFile(path).exists()) {
          missingPath = path;
          break;
        }
        if (v == toVersion) {
          break;
        }
      }
    } catch (Exception e) {
      throw new BaseException(
          ErrorCode.COMMIT_STATE_UNKNOWN,
          "Could not verify the published commit files under "
              + deltaLogDir(tableLocation)
              + " for backfill through version "
              + toVersion
              + "; retry the request.",
          e);
    }
    if (missingPath != null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Cannot backfill through version "
              + toVersion
              + ": published commit file is missing: "
              + missingPath);
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
   * Returns the exception the caller must throw for an {@code add-commit} whose {@code version} is
   * already taken. Returning (rather than throwing) keeps the {@code throw} at the call site, so
   * this stays a total function that always yields an outcome. A client that lost the response to
   * an accepted commit may safely resend; recognizing the replay lets the server report success
   * (via a rolled-back no-op) instead of a spurious conflict.
   *
   * <p>The client-generated per-commit-unique UUID in the file name is the dedup handle: same name
   * -&gt; replay, different name -&gt; conflict. This works for backfilled versions too, as long as
   * their row is still retained, which is the case that would otherwise stall a retrying writer.
   * Only a version with no row at all -- aged out of retention, or backfilled before rows were
   * retained -- defers to an out-of-transaction file-content check.
   *
   * <p>The caller must hold the table lock (see {@link RepositoryUtils#lockTableForCommit}).
   *
   * @return a {@link CommitAlreadyAcceptedException} on a recognized replay; a {@link
   *     CommitContentCheckRequiredException} when an unretained version needs a content check; or a
   *     {@link BaseException} ({@code COMMIT_VERSION_CONFLICT} for a genuine conflict, {@code
   *     INTERNAL} if the live commit window is inconsistent)
   */
  private static RuntimeException exceptionForAlreadyTakenVersion(
      Session session,
      TableInfoDAO tableInfoDAO,
      long version,
      String fileName,
      long backfilledThrough,
      DeltaCommitDAO lastCommitDAO) {
    UUID tableId = tableInfoDAO.getId();
    // The last commit is already in hand and is the version a writer most often retries; anything
    // else costs one indexed lookup.
    DeltaCommitDAO existing =
        version == lastCommitDAO.getCommitVersion()
            ? lastCommitDAO
            : findCommitByVersion(session, tableId, version).orElse(null);
    if (existing == null) {
      if (version > backfilledThrough) {
        // Versions in the live window are contiguous, and the caller holds the table write lock, so
        // no concurrent backfill can have removed this one. A gap means uc_delta_commits is
        // internally inconsistent.
        return new BaseException(
            ErrorCode.INTERNAL,
            "Inconsistent uc_delta_commits table for table "
                + tableId
                + ": no row tracked at in-range commit version "
                + version);
      }
      // Backfilled and no longer retained, so the file name is gone and the DB alone can't tell a
      // replay from a conflict. Defer to a content check.
      return new CommitContentCheckRequiredException(
          NormalizedURL.from(tableInfoDAO.getUrl()), version, fileName);
    }
    if (existing.getCommitFilename().equals(fileName)) {
      return new CommitAlreadyAcceptedException();
    }
    return new BaseException(
        ErrorCode.COMMIT_VERSION_CONFLICT,
        "Commit version already accepted. Version "
            + version
            + " was accepted with commit file "
            + existing.getCommitFilename()
            + ", but the request carried "
            + fileName
            + ".");
  }

  /**
   * Looks up the commit tracked at a specific version of a table, or empty if none. At most one row
   * can match, per the {@code (table_id, commit_version)} unique constraint on {@link
   * DeltaCommitDAO}.
   */
  private static Optional<DeltaCommitDAO> findCommitByVersion(
      Session session, UUID tableId, long commitVersion) {
    Query<DeltaCommitDAO> query =
        session.createQuery(
            "FROM DeltaCommitDAO WHERE tableId = :tableId AND commitVersion = :commitVersion",
            DeltaCommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setParameter("commitVersion", commitVersion);
    return query.uniqueResultOptional();
  }

  /**
   * Settles a {@link CommitContentCheckRequiredException} out of the transaction by comparing the
   * incoming staged commit file against the published {@code _delta_log/<version>.json} (both are
   * immutable once written, so this is safe to read outside the table lock):
   *
   * <ul>
   *   <li>identical content -&gt; a replay of the already-published commit; returns normally so the
   *       caller reports an idempotent no-op success.
   *   <li>a definitive difference -&gt; another writer won this version; throws {@code
   *       COMMIT_VERSION_CONFLICT} (409).
   *   <li>either file unreadable -&gt; {@code COMMIT_STATE_UNKNOWN} (500, retriable), so the client
   *       retries rather than receiving a false conflict.
   * </ul>
   */
  static void verifyContentReplayOrThrowConflict(
      FileOperations fileOperations, CommitContentCheckRequiredException check) {
    String publishedPath = publishedCommitPath(check.tableLocation, check.version);
    String stagedPath = stagedCommitPath(check.tableLocation, check.stagedFileName);
    boolean sameContent;
    // getFileIO can vend credentials (cloud paths) and open resources, so it is acquired inside the
    // guarded block (and closed): a vend or read failure is equally "cannot determine" and must
    // fail open to COMMIT_STATE_UNKNOWN.
    try (FileIO fileIO = fileOperations.getFileIO(check.tableLocation)) {
      sameContent =
          hasSameFileContent(fileIO.newInputFile(publishedPath), fileIO.newInputFile(stagedPath));
    } catch (Exception e) {
      throw new BaseException(
          ErrorCode.COMMIT_STATE_UNKNOWN,
          "Could not determine whether commit version "
              + check.version
              + " is a replay: unable to read the staged or published commit file for the table at "
              + check.tableLocation
              + "; retry the request.",
          e);
    }
    if (!sameContent) {
      throw new BaseException(
          ErrorCode.COMMIT_VERSION_CONFLICT,
          "Commit version already accepted. Version " + check.version + " is already published.");
    }
  }

  /**
   * Whether two files have identical content, streamed in lockstep with a fixed buffer. The staged
   * file is client-influenced, so -- unlike a client-side library -- the server must never read it
   * wholesale into memory. Reading both streams in step bounds memory to one buffer and bounds the
   * read to the smaller file: a client that inflates its staged file only forces a read up to the
   * (real, bounded) published file's length before the size mismatch surfaces as a non-equal.
   *
   * <p>A missing file makes {@code newStream()} throw, which the caller turns into {@code
   * COMMIT_STATE_UNKNOWN} -- deliberately not treated as "not equal", so a missing file never
   * becomes a false conflict.
   */
  private static boolean hasSameFileContent(InputFile a, InputFile b) throws IOException {
    try (InputStream streamA = a.newStream();
        InputStream streamB = b.newStream()) {
      byte[] bufA = new byte[CONTENT_COMPARE_BUFFER_BYTES];
      byte[] bufB = new byte[CONTENT_COMPARE_BUFFER_BYTES];
      while (true) {
        int nA = streamA.readNBytes(bufA, 0, bufA.length);
        int nB = streamB.readNBytes(bufB, 0, bufB.length);
        if (nA == 0 && nB == 0) {
          return true; // both reached EOF with all bytes equal
        }
        if (nA != nB || !Arrays.equals(bufA, 0, nA, bufB, 0, nB)) {
          return false; // differing bytes, or one file is shorter (size mismatch)
        }
      }
    }
  }

  /**
   * Deletes commit rows that have fallen more than {@link #NUM_BACKFILLED_COMMITS_RETAINED}
   * versions behind {@code latestBackfilledVersion}. The version exactly that far behind is kept. A
   * retry at an older version goes back to the staged-vs-published content check, which is the
   * behaviour from before rows were retained.
   *
   * <p>Called on every watermark advance, so in steady state there are only a handful of newly
   * aged-out rows and a single batch clears them. A large jump may leave some behind; the next
   * backfill picks them up, so this does not need to run to completion.
   *
   * @return the number of rows deleted
   */
  private static int pruneRetainedCommits(
      Session session, UUID tableId, long latestBackfilledVersion) {
    // latest - retention is still inside the window. Subtract one more, but only once latest is
    // past the window: otherwise latest - retention - 1 underflows through the non-negative
    // versions and would delete rows that must be kept.
    if (latestBackfilledVersion <= NUM_BACKFILLED_COMMITS_RETAINED) {
      return 0;
    }
    long pruneThrough = latestBackfilledVersion - NUM_BACKFILLED_COMMITS_RETAINED - 1L;
    int total = 0;
    for (int i = 0; i < MAX_DELETE_BATCHES; i++) {
      NativeQuery<?> query =
          session.createNativeQuery(
              buildBatchDeleteQuery("table_id = :tableId AND commit_version <= :pruneThrough"));
      query.setParameter("tableId", tableId);
      query.setParameter("pruneThrough", pruneThrough);
      query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
      int deleted = query.executeUpdate();
      total += deleted;
      if (deleted < NUM_COMMITS_PER_BATCH) {
        break;
      }
    }
    return total;
  }

  /**
   * Deletes commits for the specified table in a single batch.
   *
   * <p>Unlike {@link #pruneRetainedCommits(Session, UUID, long)}, this method deletes any commits
   * for the table without version filtering. The operation is limited by {@code
   * NUM_COMMITS_PER_BATCH}. Used primarily during table deletion to purge all commit history.
   *
   * @param session the Hibernate session for database operations
   * @param tableId the unique identifier of the table
   * @return the number of commits actually deleted in this batch
   */
  private static int deleteCommits(Session session, UUID tableId) {
    NativeQuery<?> query = session.createNativeQuery(buildBatchDeleteQuery("table_id = :tableId"));
    query.setParameter("tableId", tableId);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  /**
   * Builds a batch DELETE that avoids DELETE...LIMIT, which PostgreSQL does not support: an inner
   * SELECT picks up to :numCommitsPerBatch matching ids and the DELETE removes those ids.
   *
   * <p>The inner SELECT is wrapped in a derived table ({@code batch_to_delete}) purely for MySQL:
   * MySQL rejects both LIMIT directly inside an IN(...) subquery (error 1235) and a subquery that
   * selects from the table being deleted from (error 1093). The wrapper materializes the ids into a
   * temp table first, which sidesteps both. Do not flatten it to a single subquery — the H2 and
   * PostgreSQL tests will still pass, but MySQL will fail at runtime.
   */
  private static String buildBatchDeleteQuery(String whereClause) {
    return "DELETE FROM uc_delta_commits WHERE id IN ("
        + "SELECT id FROM (SELECT id FROM uc_delta_commits "
        + "WHERE "
        + whereClause
        + " "
        + "LIMIT :numCommitsPerBatch) AS batch_to_delete)";
  }

  /**
   * The highest-versioned commit row for a table, or empty if the table has no commits yet (it has
   * not been onboarded to Unity Catalog as commit coordinator).
   *
   * <p>This is the table's current version. It is not necessarily unbackfilled: rows stay after the
   * watermark passes them, and a table whose client backfills promptly sits with its newest row
   * already backfilled.
   */
  private Optional<DeltaCommitDAO> getLastCommit(Session session, UUID tableId) {
    Query<DeltaCommitDAO> query =
        session.createQuery(
            "FROM DeltaCommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
            DeltaCommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setMaxResults(1);
    return query.uniqueResultOptional();
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
      Session session,
      UUID tableId,
      TableInfoDAO tableInfoDAO,
      DeltaCommit commit,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields) {
    boolean hasUpdates = false;

    if (commit.getMetadata() != null) {
      updateTableMetadata(session, tableId, tableInfoDAO, commit.getMetadata());
      hasUpdates = true;
    }

    if (uniformFields.isPresent()) {
      DeltaUniformUtils.applyToDao(tableInfoDAO, uniformFields);
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
      validateCommitInfo(commit.getCommitInfo());
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
          UcManagedDeltaContract.validateTableIdProperty(propertiesOpt.get(), commit.getTableId());
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

  /**
   * Validates the 5 required fields of a commit-info block (version, timestamp, file name, file
   * size, file modification timestamp). Shared by {@link #validateCommit} (UC REST) and {@link
   * #applyCommitAndBackfillInSession} (Delta update path, via {@code toUcCommitInfo} first).
   */
  private static void validateCommitInfo(DeltaCommitInfo commitInfo) {
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
    // The file name is client-supplied and the server later builds the staged-commit path it reads
    // from it (_delta_log/_staged_commits/<fileName>). Require a single path segment so a name like
    // "../<v>.json" cannot traverse out of that directory.
    String fileName = commitInfo.getFileName();
    ValidationUtils.checkArgument(
        !fileName.contains("/")
            && !fileName.contains("\\")
            && !fileName.equals(".")
            && !fileName.equals(".."),
        "Field must be a single file name without path separators: %s",
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
   * Validate that the table is eligible for the commit and the commit's table URI matches.
   *
   * <ul>
   *   <li>Table eligibility ({@link #validateTable}: MANAGED + DELTA + non-null URL).
   *   <li>Table URI equality: the URI in the commit must match the registered table URL after
   *       normalization.
   *   <li>UniForm subpath: when {@code uniformFields} is present, its {@code metadata-location}
   *       must be a subpath of the table's storage root.
   *   <li>UniForm property/block consistency via {@link #validateUniformMetadataPresence}.
   * </ul>
   *
   * <p>{@code uniformFields} is the already-validated, already-normalized output of {@link
   * DeltaUniformUtils#getUniformFields(DeltaCommit)}; this method only does the table-aware checks.
   */
  private static void validateTableForCommit(
      Session session,
      DeltaCommit commit,
      TableInfoDAO tableInfoDAO,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields) {
    validateTable(tableInfoDAO);
    NormalizedURL commitTableUri = NormalizedURL.from(commit.getTableUri());
    NormalizedURL tableUri = NormalizedURL.from(tableInfoDAO.getUrl());
    ValidationUtils.checkArgument(
        commitTableUri.equals(tableUri),
        "Table URI in commit %s does not match the table path %s",
        commit.getTableUri(),
        tableInfoDAO.getUrl());
    uniformFields.ifPresent(
        uf -> DeltaUniformUtils.requireMetadataLocationSubpath(uf.metadataLocation(), tableUri));
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
    DeltaUniformUtils.validateConsistency(effectiveProperties, commit.getUniform() != null);
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
    boolean allDeleted = false;
    int numDeleted = 0;
    for (int i = 0; i < MAX_DELETE_BATCHES; i++) {
      int deleted = deleteCommits(session, tableId);
      numDeleted += deleted;
      if (deleted < NUM_COMMITS_PER_BATCH) {
        allDeleted = true;
        break;
      }
    }
    // Retained backfilled rows are expected here, so the live-window limit is not the yardstick:
    // only a table that outgrew the retention window is worth reporting.
    if (numDeleted > NUM_BACKFILLED_COMMITS_RETAINED + MAX_NUM_COMMITS_PER_TABLE) {
      LOGGER.error(
          "Purged {} commits for table {}, which exceeds the retained commit window",
          numDeleted,
          tableId);
    }
    if (!allDeleted) {
      LOGGER.error(
          "Failed to purge all commits for table {} after {} batches", tableId, MAX_DELETE_BATCHES);
    }
  }
}
