package io.unitycatalog.server.persist;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.Metadata;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.Constants;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitRepository {
  private static final CommitRepository INSTANCE = new CommitRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

  // The maximum number of commits per table.
  public static final Integer MAX_NUM_COMMITS_PER_TABLE = 50;
  private static final Integer NUM_COMMITS_PER_BATCH = 100;

  public static CommitRepository getInstance() {
    return INSTANCE;
  }

  public void saveCommit(Session session, Commit commit) {
    CommitDAO commitDAO = CommitDAO.from(commit);
    session.persist(commitDAO);
  }

  public void backfillCommits(
      Session session, UUID tableId, Long upTo, CommitDAO firstCommit, Long highestCommitVersion) {
    assert upTo >= firstCommit.getCommitVersion();
    assert upTo < highestCommitVersion;
    long numCommitsToDelete = upTo - firstCommit.getCommitVersion() + 1;
    if (numCommitsToDelete <= 0) {
      return;
    }
    // Retry backfilling 5 times to prioritize cleaning of the commit table and log bugs where there
    // are more
    // commits in the table than MAX_NUM_COMMITS_PER_TABLE
    for (int i = 0; i < 5 && numCommitsToDelete > 0; i++) {
      numCommitsToDelete -= deleteCommits(session, tableId, upTo);
      if (numCommitsToDelete > 0) {
        LOGGER.error(
            "Failed to backfill commits for tableId: {}, upTo: {}, in batch: {}, commits left: {}",
            tableId,
            upTo,
            i,
            numCommitsToDelete);
      }
    }
  }

  public int deleteCommits(Session session, UUID tableId, Long upTo) {
    NativeQuery<CommitDAO> query =
        session.createNativeQuery(
            "DELETE FROM uc_commits WHERE table_id = :tableId AND commit_version <= :upTo LIMIT :numCommitsPerBatch",
            CommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setParameter("upTo", upTo);
    query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
    return query.executeUpdate();
  }

  public void markCommitAsLatestBackfilled(Session session, UUID tableId, Long commitVersion) {
    NativeQuery<CommitDAO> query =
        session.createNativeQuery(
            "UPDATE uc_commits SET is_backfilled_latest_commit = true WHERE table_id = :tableId "
                + "AND commit_version = :commitVersion",
            CommitDAO.class);
    query.setParameter("tableId", tableId);
    query.setParameter("commitVersion", commitVersion);
    query.executeUpdate();
  }

  public List<CommitDAO> getFirstAndLastCommits(Session session, UUID tableId) {
    // Use native SQL to get the first and last commits since HQL doesn't support UNION ALL
    String sql =
        "(SELECT * FROM uc_commits WHERE table_id = :tableId ORDER BY commit_version ASC LIMIT 1) "
            + "UNION ALL "
            + "(SELECT * FROM uc_commits WHERE table_id = :tableId ORDER BY commit_version DESC LIMIT 1)";

    Query<CommitDAO> query = session.createNativeQuery(sql, CommitDAO.class);
    query.setParameter("tableId", tableId);
    List<CommitDAO> result = query.getResultList();
    // Sort to ensure the first commit is at index 0
    result.sort(Comparator.comparing(CommitDAO::getCommitVersion));
    return result;
  }

  public void updateTableMetadata(Session session, Commit commit) {
    TableInfoDAO tableInfoDAO =
        session.get(TableInfoDAO.class, UUID.fromString(commit.getTableId()));
    Metadata metadata = commit.getMetadata();
    // Update properties
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    PropertyDAO.from(metadata.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::persist);
    // Update columns
    tableInfoDAO.setColumns(ColumnInfoDAO.fromList(metadata.getSchema().getColumns()));
    // Update comment
    tableInfoDAO.setComment(metadata.getDescription());
    // Update name
    tableInfoDAO.setName(metadata.getName());
    session.merge(tableInfoDAO);
  }

  /**
   * @param tableId the table id to get the latest commits for
   * @return the latest commits for the table sorted by commit version in descending order up to
   *     MAX_NUM_COMMITS_PER_TABLE
   */
  public List<CommitDAO> getLatestCommits(UUID tableId) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Query<CommitDAO> query =
          session.createQuery(
              "FROM CommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
              CommitDAO.class);
      query.setParameter("tableId", tableId);
      query.setMaxResults(MAX_NUM_COMMITS_PER_TABLE);
      return query.list();
    }
  }
}
