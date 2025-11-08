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
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

/** Repository for managing coordinated commits for managed Delta tables in Unity Catalog. */
public class CommitRepository {

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
              throw new BaseException(ErrorCode.UNIMPLEMENTED, "Backfill is not implemented yet!");
            } else {
              handleNormalCommit(session, tableId, commit, lastCommit);
            }
          }
          return null;
        },
        "Error committing to table: " + commit.getTableId(),
        /* readOnly = */ false);
  }

  private static void handleOnboardingCommit(
      Session session, UUID tableId, TableInfoDAO tableInfoDAO, Commit commit) {
    CommitInfo commitInfo = commit.getCommitInfo();
    ValidationUtils.checkArgument(
        commitInfo != null,
        "Field can not be null: %s in onboarding commit",
        Commit.JSON_PROPERTY_COMMIT_INFO);
    saveCommit(session, tableId, commitInfo);
    // TODO: update table metadata
  }

  private static void handleNormalCommit(
      Session session, UUID tableId, Commit commit, CommitDAO lastCommit) {
    CommitInfo commitInfo = Objects.requireNonNull(commit.getCommitInfo());
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
    // TODO: check commit limit after implementing backfill
    saveCommit(session, tableId, commitInfo);
    // TODO: update table metadata
  }

  private static void saveCommit(Session session, UUID tableId, CommitInfo commitInfo) {
    CommitDAO commitDAO = CommitDAO.from(tableId, commitInfo);
    session.persist(commitDAO);
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
        commit.getTableUri() != null && commit.getTableUri().equals(tableInfoDAO.getUrl()),
        "Table URI in commit does not match the table path");
  }
}
