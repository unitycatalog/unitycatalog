package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.zip.CRC32;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A migration step that changes rows, written with plain JDBC. It is safe to repeat: when {@link
 * #alreadyDone} finds the change in place, the step is recorded without running {@link #apply}.
 *
 * <p>By default the whole step runs in one transaction. A step on a table that may be large returns
 * true from {@link #batched()} and calls {@link #updateInBatches} from {@link #apply}: each batch
 * is committed, so once the failed run is repaired, a rerun continues where the last one stopped.
 */
public abstract class DataStep extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStep.class);

  /** Whether the change is already in place (for example, no row is left to change). */
  protected abstract boolean alreadyDone(Connection connection) throws SQLException;

  /**
   * Makes the change. Runs in one transaction unless {@link #batched()}; then it calls {@link
   * #updateInBatches}.
   */
  protected abstract void apply(Connection connection) throws SQLException;

  /** Whether {@link #apply} commits in batches instead of in one transaction. */
  protected boolean batched() {
    return false;
  }

  /** The database the step runs on, to pick SQL that differs between databases. */
  protected static DatabaseKind databaseKind(Connection connection) throws SQLException {
    return DatabaseKind.fromJdbcUrl(connection.getMetaData().getURL());
  }

  /**
   * Runs {@code sql} until it changes no row, committing after each run, and returns the rows
   * changed in total. {@code sql} is an {@code UPDATE} with one {@code ?}, the batch size, that
   * changes at most that many rows and only rows not yet changed (for example {@code WHERE id IN
   * (SELECT id FROM t WHERE c IS NULL LIMIT ?)}); the SQL for that differs between databases. A
   * changed row must no longer match: a new value that can be NULL in that example never stops.
   */
  protected final long updateInBatches(Connection connection, String sql, int batchSize)
      throws SQLException {
    if (!batched()) {
      throw new IllegalStateException(
          name() + " calls updateInBatches, which commits; override batched() to return true");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("The batch size must be positive, not " + batchSize);
    }
    long total = 0;
    try (PreparedStatement update = connection.prepareStatement(sql)) {
      update.setInt(1, batchSize);
      int changed;
      while ((changed = update.executeUpdate()) > 0) {
        if (!connection.getAutoCommit()) {
          connection.commit();
        }
        total += changed;
        LOGGER.info("{}: changed {} rows in this batch, {} so far", name(), changed, total);
      }
    }
    return total;
  }

  @Override
  public final void migrate(Context context) throws SQLException {
    Connection connection = context.getConnection();
    if (alreadyDone(connection)) {
      LOGGER.info("{}: already done; recording it without changes", name());
      return;
    }
    LOGGER.info("{}: started", name());
    apply(connection);
    LOGGER.info("{}: done", name());
  }

  @Override
  public final boolean canExecuteInTransaction() {
    return !batched();
  }

  /** Stable across builds: an edit to a merged step is refused by the merge check instead. */
  @Override
  public final Integer getChecksum() {
    CRC32 crc = new CRC32();
    crc.update(
        (getVersion().getVersion() + "|" + getDescription()).getBytes(StandardCharsets.UTF_8));
    return (int) crc.getValue();
  }

  private String name() {
    return "Step " + getVersion() + " (" + getDescription() + ")";
  }
}
