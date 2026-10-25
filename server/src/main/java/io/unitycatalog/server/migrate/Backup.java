package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The backup before the upgrade: a copy of the whole Unity Catalog database (its schema history and
 * Casbin's table included) on the same database server, taken while the servers are stopped, which
 * {@link SchemaMigrator} takes before a run's first change and restores when the run fails.
 *
 * <ul>
 *   <li>PostgreSQL: the database {@code <db>_uc_backup}, made with {@code CREATE DATABASE ...
 *       TEMPLATE <db>} under another name, given {@code <db>}'s own settings, grants, owner and
 *       connection limit, then renamed; the restore drops {@code <db>} and renames the copy to it.
 *       Both need every session on {@code <db>} closed, so they run on a connection to the
 *       companion database {@code <db>_uc_upgrade}, with the pool's connections closed.
 *   <li>MySQL: the database {@code <db>_uc_backup}, with each table created from {@code SHOW CREATE
 *       TABLE}, its rows copied in one transaction, then the table {@code uc_backup_complete},
 *       which marks the copy complete; the restore moves {@code <db>}'s tables to {@code
 *       <db>_uc_trash} and the copies back in one {@code RENAME TABLE}, which MySQL runs whole or
 *       not at all.
 *   <li>H2 (a file database): the file {@code <file>_uc_backup.mv.db}, copied with the database
 *       closed; the restore moves it back.
 * </ul>
 *
 * <p>In a Helm chart revision, a restore writes the restored mark, naming the release and the
 * revision, in the companion database {@code <db>_uc_upgrade} (H2: the file {@code
 * <file>_uc_upgrade.properties}); later runs of that release and revision refuse, and a run of
 * another revision clears it.
 *
 * <p>A class of its own: three databases' copy, restore and mark would double the length of {@link
 * SchemaMigrator}'s run.
 */
final class Backup {

  /** The environment variable that names the Helm chart revision. */
  static final String CHART_REVISION_ENV = "UC_CHART_REVISION";

  private static final Logger LOGGER = LoggerFactory.getLogger(Backup.class);
  private static final String COPY = "_uc_backup";
  private static final String COMPANION = "_uc_upgrade";
  private static final String MARK_TABLE = "uc_restored";
  private static final String TRASH = "_uc_trash";

  /** MySQL: the copy's table that marks it complete, created after its rows are copied. */
  private static final String COMPLETE_TABLE = "uc_backup_complete";

  /** Whether a copy exists, and whether it was taken in full. */
  private enum CopyState {
    NONE,
    INCOMPLETE,
    COMPLETE
  }

  /**
   * PostgreSQL's settings whose value is a list, which {@code pg_db_role_setting} holds as quoted
   * names (as pg_dump's list of them).
   */
  private static final Set<String> LIST_SETTINGS =
      Set.of(
          "local_preload_libraries",
          "search_path",
          "session_preload_libraries",
          "shared_preload_libraries",
          "temp_tablespaces",
          "unix_socket_directories");

  /** One element of a list setting's value: group 1 a quoted name, group 2 a plain one. */
  private static final Pattern LIST_ELEMENT =
      Pattern.compile("\\s*(?:\"((?:[^\"]|\"\")*)\"|([^,]+))\\s*(?:,|$)");

  /** A PostgreSQL or MySQL JDBC URL: group 2 is the database. */
  private static final Pattern URL_DATABASE =
      Pattern.compile("(jdbc:(?:postgresql|mysql):(?://[^/]*/)?)([^/?;]*)(.*)");

  /** What a run does while the pool's connections are closed. */
  private interface Work {
    void run() throws SQLException, IOException;
  }

  private final HikariDataSource pool;
  private final DatabaseKind kind;
  private final Duration lockWait;
  private final String chartRevision; // null: outside Helm, no mark
  private String name; // the database (H2: its file without .mv.db), read on first use

  /**
   * @param dataSource the server's pool on the database, a {@link HikariDataSource}
   * @param lockWait how long a statement waits for a lock, and the run for the pool to close
   * @param chartRevision the Helm chart revision, {@code null} outside Helm
   */
  Backup(DataSource dataSource, DatabaseKind kind, Duration lockWait, String chartRevision) {
    if (!(dataSource instanceof HikariDataSource hikari)) {
      throw new IllegalArgumentException("The backup before the upgrade needs a Hikari pool");
    }
    this.pool = hikari;
    this.kind = kind;
    this.lockWait = lockWait;
    this.chartRevision = chartRevision;
  }

  /**
   * Refuses a run that can't take the backup (an in-memory H2 database), or a run of the release
   * {@code release} in the chart revision whose restore the mark records; clears a mark of another
   * revision.
   */
  void checkBeforeRun(String release) {
    name(); // refuses an in-memory H2 database
    if (chartRevision == null) {
      return;
    }
    try {
      List<String> mark = mark();
      if (mark.equals(List.of(release, chartRevision))) {
        throw new IllegalStateException(restoredRefusal(release));
      }
      if (!mark.isEmpty()) {
        clearMark();
        LOGGER.info(
            "Cleared the restored mark of release {}, chart revision {}: this is revision {}.",
            mark.get(0),
            mark.get(1),
            chartRevision);
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("Could not read the restored mark: " + e.getMessage(), e);
    }
  }

  /**
   * The refusal of a run of the release {@code release} in the chart revision whose restore the
   * mark records, or {@code null}. Changes nothing in the database.
   */
  String refusalForTheRestoredMark(String release) {
    try {
      return chartRevision != null && mark().equals(List.of(release, chartRevision))
          ? restoredRefusal(release)
          : null;
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("Could not read the restored mark: " + e.getMessage(), e);
    }
  }

  private String restoredRefusal(String release) {
    return "The upgrade to "
        + release
        + " failed and the database was restored in chart revision "
        + chartRevision
        + "; fix the cause, then run helm upgrade again (a new revision clears this).";
  }

  /**
   * Takes the backup. An existing copy is kept when {@code reuse} (the database is partway through
   * this release's steps, so the copy is from before them) and replaced otherwise. Refuses to
   * replace a copy holding tables the database lacks, and to keep an incomplete copy.
   */
  void take(boolean reuse) {
    try {
      CopyState copy = copyState();
      if (copy == CopyState.COMPLETE && reuse) {
        LOGGER.info("Kept the backup {} taken before this upgrade started.", copyName());
        return;
      }
      if (copy == CopyState.INCOMPLETE && reuse) {
        throw new IllegalStateException(
            "The backup "
                + copyName()
                + " is incomplete: taking it was interrupted. The database is partway through this"
                + " release's steps, so there is no backup from before them. Drop "
                + copyName()
                + " to upgrade with a backup of the database as it is now, or restore a backup of"
                + " your own.");
      }
      if (copy == CopyState.COMPLETE) {
        refuseWhenTheDatabaseLacksTheCopysTables();
      }
      if (copy != CopyState.NONE) {
        drop();
      }
      switch (kind) {
        case POSTGRESQL -> takePostgres();
        case MYSQL -> takeMySql();
        case H2 -> takeH2();
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException(
          "Could not back up the database before the upgrade, which changed nothing: "
              + e.getMessage(),
          e);
    }
    LOGGER.info("Backed up the database to {} before the upgrade.", copyName());
  }

  /** Puts the copy back in place of the database; the copy is gone after. */
  void restore() {
    try {
      switch (kind) {
        case POSTGRESQL -> restorePostgres();
        case MYSQL -> restoreMySql();
        case H2 -> restoreH2();
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException(
          "Could not restore the backup: " + e.getMessage() + ". " + restoreByHand(), e);
    }
    LOGGER.info("Restored the database from the backup {}.", copyName());
  }

  /** Drops the copy, if any. */
  void drop() {
    try {
      switch (kind) {
        case POSTGRESQL -> {
          try (Connection connection = companionConnection();
              Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + quote(copyName()));
          }
        }
        case MYSQL -> {
          try (Connection connection = connection();
              Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + quote(copyName()));
          }
        }
        case H2 -> Files.deleteIfExists(h2File(COPY));
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException(
          "Could not drop the backup " + copyName() + ": " + e.getMessage(), e);
    }
  }

  /** Writes the restored mark for the release {@code release}, in a chart revision only. */
  void markRestored(String release) {
    if (chartRevision == null) {
      return;
    }
    try {
      if (kind == DatabaseKind.H2) {
        Properties mark = new Properties();
        mark.setProperty("release", release);
        mark.setProperty("revision", chartRevision);
        mark.setProperty("restored_at", Instant.now().toString());
        try (Writer out = Files.newBufferedWriter(h2MarkFile())) {
          mark.store(out, "The upgrade failed and the database was restored from its backup.");
        }
        return;
      }
      clearMark();
      try (Connection connection = markConnection();
          PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO "
                      + markTable()
                      + " ("
                      + quote("release")
                      + ", revision, restored_at) VALUES (?, ?, CURRENT_TIMESTAMP)")) {
        insert.setString(1, release);
        insert.setString(2, chartRevision);
        insert.executeUpdate();
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException(
          "The database was restored from the backup taken before the upgrade, but the restored"
              + " mark could not be written: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * PostgreSQL: a connection to the companion database, which this creates if it is missing. The
   * run lock and the backup's statements use it, so none of the run's sessions is on the database.
   */
  Connection companionConnection() throws SQLException {
    String companion = name() + COMPANION;
    try {
      return connect(companion);
    } catch (SQLException e) {
      if (!"3D000".equals(e.getSQLState())) { // invalid_catalog_name: missing
        throw e;
      }
    }
    try (Connection connection = connect(name());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + quote(companion));
    } catch (SQLException e) {
      if (!"42P04".equals(e.getSQLState())) { // duplicate_database: another server created it
        throw e;
      }
    }
    return connect(companion);
  }

  /**
   * Copies the database under another name, gives the copy what {@code TEMPLATE} doesn't copy, then
   * renames it: a copy under the backup's name is complete.
   */
  private void takePostgres() throws SQLException, IOException {
    String partial = copyName() + "_part";
    try (Connection connection = companionConnection();
        Statement statement = connection.createStatement()) {
      List<String> attributes = databaseAttributes(connection, partial);
      statement.execute("DROP DATABASE IF EXISTS " + quote(partial)); // a killed run's
      withPoolClosed(
          () ->
              statement.execute(
                  "CREATE DATABASE " + quote(partial) + " TEMPLATE " + quote(name())));
      try {
        for (String sql : attributes) {
          statement.execute(sql);
        }
        statement.execute("ALTER DATABASE " + quote(partial) + " RENAME TO " + quote(copyName()));
      } catch (SQLException e) {
        try {
          statement.execute("DROP DATABASE " + quote(partial));
        } catch (SQLException cleanup) {
          e.addSuppressed(cleanup);
        }
        throw e;
      }
    } catch (SQLException e) {
      throw otherSessions(e);
    }
  }

  /**
   * PostgreSQL: the statements that give {@code target} the database's owner, connection limit,
   * grants and settings (its own and its roles'), which {@code CREATE DATABASE ... TEMPLATE}
   * doesn't copy.
   */
  private List<String> databaseAttributes(Connection connection, String target)
      throws SQLException {
    String database = quote(target);
    List<String> statements = new ArrayList<>();
    for (List<String> row :
        query(
            connection,
            "SELECT pg_get_userbyid(datdba), datconnlimit, datacl IS NULL FROM pg_database"
                + " WHERE datname = ?")) {
      statements.add("ALTER DATABASE " + database + " OWNER TO " + quote(row.get(0)));
      statements.add("ALTER DATABASE " + database + " CONNECTION LIMIT " + row.get(1));
      if (row.get(2).equals("f")) { // grants of its own: none by default
        statements.add("REVOKE ALL ON DATABASE " + database + " FROM PUBLIC");
      }
    }
    for (List<String> row :
        query(
            connection,
            "SELECT r.rolname, a.privilege_type, a.is_grantable FROM pg_database d,"
                + " aclexplode(d.datacl) a LEFT JOIN pg_roles r ON r.oid = a.grantee"
                + " WHERE d.datname = ?")) {
      statements.add(
          "GRANT "
              + row.get(1)
              + " ON DATABASE "
              + database
              + " TO "
              + (row.get(0) == null ? "PUBLIC" : quote(row.get(0)))
              + (row.get(2).equals("t") ? " WITH GRANT OPTION" : ""));
    }
    for (List<String> row :
        query(
            connection,
            "SELECT r.rolname, unnest(s.setconfig) FROM pg_db_role_setting s JOIN pg_database d"
                + " ON d.oid = s.setdatabase LEFT JOIN pg_roles r ON r.oid = s.setrole"
                + " WHERE d.datname = ?")) {
      String[] setting = row.get(1).split("=", 2);
      statements.add(
          (row.get(0) == null
                  ? "ALTER DATABASE " + database
                  : "ALTER ROLE " + quote(row.get(0)) + " IN DATABASE " + database)
              + " SET "
              + quote(setting[0])
              + " TO "
              + settingValue(setting[0], setting[1]));
    }
    return statements;
  }

  /** The rows of {@code sql}, with the database's name as its parameter. */
  private List<List<String>> query(Connection connection, String sql) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    try (PreparedStatement query = connection.prepareStatement(sql)) {
      query.setString(1, name());
      try (ResultSet rs = query.executeQuery()) {
        while (rs.next()) {
          List<String> row = new ArrayList<>();
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            row.add(rs.getString(i));
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }

  /** The value of the PostgreSQL setting {@code setting} as SQL: a string per list element. */
  private static String settingValue(String setting, String value) {
    if (!LIST_SETTINGS.contains(setting)) {
      return literal(value);
    }
    List<String> elements = new ArrayList<>();
    Matcher element = LIST_ELEMENT.matcher(value);
    while (element.find()) {
      elements.add(
          literal(
              element.group(1) != null
                  ? element.group(1).replace("\"\"", "\"")
                  : element.group(2).strip()));
    }
    return String.join(", ", elements);
  }

  private static String literal(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private void restorePostgres() throws SQLException, IOException {
    try (Connection connection = companionConnection();
        Statement statement = connection.createStatement()) {
      withPoolClosed(
          () -> {
            statement.execute("DROP DATABASE " + quote(name()));
            statement.execute(
                "ALTER DATABASE " + quote(copyName()) + " RENAME TO " + quote(name()));
          });
    } catch (SQLException e) {
      throw otherSessions(e);
    }
  }

  /** PostgreSQL's error for another session on the database, with what to do; others as is. */
  private static SQLException otherSessions(SQLException e) {
    if (!"55006".equals(e.getSQLState())) { // object_in_use
      return e;
    }
    return new SQLException(
        "other sessions are connected to the database ("
            + e.getMessage().replaceAll("\\s+", " ").strip()
            + "). Stop the other servers on the database, then start the server again",
        e.getSQLState(),
        e);
  }

  private void takeMySql() throws SQLException {
    try (Connection connection = connection()) {
      List<String> tables = tables(connection, name());
      execute(connection, "CREATE DATABASE " + quote(copyName()));
      // SHOW CREATE TABLE names the table and the tables its foreign keys point at without the
      // database: run it in the copy's. (A statement runs in the database current when it was
      // created.)
      connection.setCatalog(copyName());
      try (Statement statement = connection.createStatement()) {
        // A table's foreign keys may point at tables created after it.
        statement.execute("SET FOREIGN_KEY_CHECKS = 0");
        for (String table : tables) {
          try (ResultSet rs =
              statement.executeQuery("SHOW CREATE TABLE " + quote(name()) + "." + quote(table))) {
            rs.next();
            statement.execute(rs.getString(2));
          }
        }
        statement.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        for (String table : tables) {
          statement.execute(
              "INSERT INTO "
                  + quote(table)
                  + " SELECT * FROM "
                  + quote(name())
                  + "."
                  + quote(table));
        }
        statement.execute("COMMIT");
        statement.execute(
            "CREATE TABLE "
                + quote(copyName())
                + "."
                + quote(COMPLETE_TABLE)
                + " (taken_at TIMESTAMP NOT NULL)");
      } catch (SQLException e) {
        try {
          execute(connection, "ROLLBACK");
          connection.setCatalog(name());
          // A partial copy would be kept by a later run partway through the steps.
          execute(connection, "DROP DATABASE " + quote(copyName()));
        } catch (SQLException cleanup) {
          e.addSuppressed(cleanup);
        }
        throw e;
      } finally {
        connection.setCatalog(name());
        execute(connection, "SET FOREIGN_KEY_CHECKS = 1");
      }
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  /**
   * Moves the database's tables to {@code <db>_uc_trash} and the copy's back in one {@code RENAME
   * TABLE}: a restore that fails or is killed leaves either database whole.
   */
  private void restoreMySql() throws SQLException {
    String trash = name() + TRASH;
    try (Connection connection = connection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET FOREIGN_KEY_CHECKS = 0");
      try {
        statement.execute("DROP DATABASE IF EXISTS " + quote(trash)); // a killed restore's
        statement.execute("CREATE DATABASE " + quote(trash));
        List<String> renames = new ArrayList<>();
        for (String table : tables(connection, name())) {
          renames.add(move(table, name(), trash));
        }
        for (String table : tables(connection, copyName())) {
          if (!table.equals(COMPLETE_TABLE)) {
            renames.add(move(table, copyName(), name()));
          }
        }
        statement.execute("RENAME TABLE " + String.join(", ", renames));
        statement.execute("DROP DATABASE " + quote(trash));
        statement.execute("DROP DATABASE " + quote(copyName()));
      } finally {
        statement.execute("SET FOREIGN_KEY_CHECKS = 1");
      }
    }
  }

  /**
   * MySQL: the {@code RENAME TABLE} pair that moves {@code table} from {@code from} to {@code to}.
   */
  private String move(String table, String from, String to) {
    return quote(from) + "." + quote(table) + " TO " + quote(to) + "." + quote(table);
  }

  /**
   * MySQL: refuses when the database lacks tables that its complete copy has, as a restore cut
   * short by a release before this one could leave it: the copy is then the only good one.
   */
  private void refuseWhenTheDatabaseLacksTheCopysTables() throws SQLException {
    if (kind != DatabaseKind.MYSQL) {
      return;
    }
    try (Connection connection = connection()) {
      List<String> missing = tables(connection, copyName());
      missing.remove(COMPLETE_TABLE);
      missing.removeAll(tables(connection, name()));
      if (!missing.isEmpty()) {
        throw new IllegalStateException(
            "The database lacks tables that its backup "
                + copyName()
                + " has ("
                + String.join(", ", missing)
                + "), so the server keeps the backup. "
                + restoreByHand());
      }
    }
  }

  /** The base tables of the MySQL database {@code database}. */
  private static List<String> tables(Connection connection, String database) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (PreparedStatement query =
        connection.prepareStatement(
            "SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = ? AND table_type = 'BASE TABLE'")) {
      query.setString(1, database);
      try (ResultSet rs = query.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
    }
    return tables;
  }

  private void takeH2() throws SQLException, IOException {
    withPoolClosed(
        () -> {
          // Copied under another name first: a partial copy would be kept by a later run partway
          // through the steps.
          Path partial = h2File(COPY + ".part");
          Files.copy(h2File(""), partial, StandardCopyOption.REPLACE_EXISTING);
          Files.move(
              partial,
              h2File(COPY),
              StandardCopyOption.REPLACE_EXISTING,
              StandardCopyOption.ATOMIC_MOVE);
        });
  }

  private void restoreH2() throws SQLException, IOException {
    withPoolClosed(
        () -> {
          Files.move(
              h2File(COPY),
              h2File(""),
              StandardCopyOption.REPLACE_EXISTING,
              StandardCopyOption.ATOMIC_MOVE);
        });
  }

  /** The H2 database file, or the backup's file with {@code suffix}. */
  private Path h2File(String suffix) {
    return Path.of(name() + suffix + ".mv.db");
  }

  private Path h2MarkFile() {
    return Path.of(name() + COMPANION + ".properties");
  }

  /**
   * Closes every connection of the pool, runs {@code work}, then lets the pool connect again. The
   * pool opens no new connection meanwhile: its minimum of idle connections is 0. H2's database is
   * closed too.
   */
  private void withPoolClosed(Work work) throws SQLException, IOException {
    int minimumIdle = pool.getMinimumIdle();
    pool.setMinimumIdle(0);
    // H2 closes its database with its last connection, in the thread that closes it: this one
    // keeps it open until the pool's have closed (in the pool's own thread), then SHUTDOWN closes
    // it in this thread, before the work.
    try (Connection h2 =
        kind == DatabaseKind.H2
            ? DriverManager.getConnection(pool.getJdbcUrl(), credentials())
            : null) {
      HikariPoolMXBean connections = pool.getHikariPoolMXBean();
      long deadline = System.nanoTime() + lockWait.toNanos();
      // Closes the idle connections now and the others when they return to the pool.
      connections.softEvictConnections();
      while (connections.getTotalConnections() > 0 || (h2 != null && otherH2Sessions(h2))) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(
              "A connection of the server's pool stayed in use for "
                  + SchemaMigrator.lockWaitSeconds(lockWait)
                  + " seconds; the backup needs them all closed");
        }
        Thread.sleep(50);
        connections.softEvictConnections();
      }
      if (h2 != null) {
        try (Statement statement = h2.createStatement()) {
          statement.execute("SHUTDOWN");
        }
      }
      work.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while closing the pool's connections", e);
    } finally {
      pool.setMinimumIdle(minimumIdle);
    }
  }

  /** Whether sessions other than {@code connection}'s are open on the H2 database. */
  private static boolean otherH2Sessions(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS")) {
      rs.next();
      return rs.getInt(1) > 1;
    }
  }

  /**
   * Whether the copy exists and is complete. PostgreSQL's {@code CREATE DATABASE} and H2's move are
   * whole or nothing; MySQL's copy is complete once it has {@link #COMPLETE_TABLE}.
   */
  private CopyState copyState() throws SQLException {
    return switch (kind) {
      case POSTGRESQL -> {
        try (Connection connection = companionConnection()) {
          yield exists(connection, "SELECT 1 FROM pg_database WHERE datname = ?")
              ? CopyState.COMPLETE
              : CopyState.NONE;
        }
      }
      case MYSQL -> {
        try (Connection connection = connection()) {
          if (!exists(
              connection, "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
            yield CopyState.NONE;
          }
          yield exists(
                  connection,
                  "SELECT 1 FROM information_schema.tables WHERE table_schema = ?"
                      + " AND table_name = '%s'".formatted(COMPLETE_TABLE))
              ? CopyState.COMPLETE
              : CopyState.INCOMPLETE;
        }
      }
      case H2 -> Files.exists(h2File(COPY)) ? CopyState.COMPLETE : CopyState.NONE;
    };
  }

  /** Whether {@code sql}, with the copy's name as its parameter, returns a row. */
  private boolean exists(Connection connection, String sql) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(sql)) {
      query.setString(1, copyName());
      try (ResultSet rs = query.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** The restored mark as its release and revision; empty without one. */
  private List<String> mark() throws SQLException, IOException {
    if (kind == DatabaseKind.H2) {
      if (!Files.exists(h2MarkFile())) {
        return List.of();
      }
      Properties mark = new Properties();
      try (Reader in = Files.newBufferedReader(h2MarkFile())) {
        mark.load(in);
      }
      String release = mark.getProperty("release");
      String revision = mark.getProperty("revision");
      // A file without them: a crash while writing it.
      return release == null || revision == null ? List.of() : List.of(release, revision);
    }
    try (Connection connection = markConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT " + quote("release") + ", revision FROM " + markTable())) {
      return rs.next() ? List.of(rs.getString(1), rs.getString(2)) : List.of();
    }
  }

  private void clearMark() throws SQLException, IOException {
    if (kind == DatabaseKind.H2) {
      Files.deleteIfExists(h2MarkFile());
      return;
    }
    try (Connection connection = markConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM " + markTable());
    }
  }

  /**
   * PostgreSQL and MySQL: a connection on which {@link #markTable()} exists, creating the companion
   * database and the table if they are missing.
   */
  private Connection markConnection() throws SQLException {
    Connection connection = kind == DatabaseKind.POSTGRESQL ? companionConnection() : connection();
    try (Statement statement = connection.createStatement()) {
      if (kind == DatabaseKind.MYSQL) {
        statement.execute("CREATE DATABASE IF NOT EXISTS " + quote(name() + COMPANION));
      }
      statement.execute(
          "CREATE TABLE IF NOT EXISTS "
              + markTable()
              + " ("
              + quote("release")
              + " VARCHAR(64) NOT NULL, revision VARCHAR(64) NOT NULL,"
              + " restored_at TIMESTAMP NOT NULL)");
    } catch (SQLException | RuntimeException e) {
      connection.close();
      throw e;
    }
    return connection;
  }

  /**
   * The mark's table: in the companion database, which MySQL names in the statement. PostgreSQL's
   * is in {@code public}: the URL's {@code currentSchema} is a schema of the database only.
   */
  private String markTable() {
    return kind == DatabaseKind.MYSQL
        ? quote(name() + COMPANION) + "." + MARK_TABLE
        : "public." + MARK_TABLE;
  }

  /** A connection of the pool, out of a transaction, waiting at most {@link #lockWait}. */
  private Connection connection() throws SQLException {
    Connection connection = pool.getConnection();
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(true);
      statement.execute(SchemaMigrator.lockWaitSql(kind, lockWait));
    } catch (SQLException | RuntimeException e) {
      connection.close();
      throw e;
    }
    return connection;
  }

  /** A PostgreSQL connection of its own to {@code database}, with the pool's URL and user. */
  private Connection connect(String database) throws SQLException {
    Connection connection = DriverManager.getConnection(url(database), credentials());
    try (Statement statement = connection.createStatement()) {
      statement.execute(SchemaMigrator.lockWaitSql(kind, lockWait));
    } catch (SQLException | RuntimeException e) {
      connection.close();
      throw e;
    }
    return connection;
  }

  /** The pool's JDBC URL with {@code database} in place of the database's name. */
  private String url(String database) {
    Matcher url = URL_DATABASE.matcher(pool.getJdbcUrl());
    if (!url.matches() || !url.group(2).equals(name())) {
      throw new IllegalStateException(
          "The backup before the upgrade needs the database's name in hibernate.connection.url,"
              + " for example jdbc:postgresql://localhost:5432/"
              + name());
    }
    return url.group(1) + database + url.group(3);
  }

  private Properties credentials() {
    Properties properties = new Properties();
    properties.putAll(pool.getDataSourceProperties());
    if (pool.getUsername() != null) {
      properties.setProperty("user", pool.getUsername());
    }
    if (pool.getPassword() != null) {
      properties.setProperty("password", pool.getPassword());
    }
    return properties;
  }

  /** The database's name; for H2 its file's path without {@code .mv.db}. */
  private String name() {
    if (name == null) {
      String sql =
          switch (kind) {
            case POSTGRESQL -> "SELECT current_database()";
            case MYSQL -> "SELECT DATABASE()";
            case H2 -> "SELECT DATABASE_PATH()";
          };
      try (Connection connection = pool.getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        rs.next();
        name = rs.getString(1);
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Could not read the database's name for the backup: " + e.getMessage(), e);
      }
      if (name == null) {
        throw new IllegalStateException(
            "The backup before the upgrade (server.migration.backup) needs an H2 file database"
                + " (jdbc:h2:file:...), but this one is in memory. Turn the backup off, or use a"
                + " file database.");
      }
    }
    return name;
  }

  private String copyName() {
    return kind == DatabaseKind.H2 ? h2File(COPY).toString() : name() + COPY;
  }

  /** What to do when the restore failed: where the copy is, and how to put it back. */
  private String restoreByHand() {
    return switch (kind) {
      case POSTGRESQL ->
          "The copy is the database "
              + copyName()
              + ". With every server stopped, run DROP DATABASE "
              + quote(name())
              + "; ALTER DATABASE "
              + quote(copyName())
              + " RENAME TO "
              + quote(name())
              + ";";
      case MYSQL ->
          "The copy's tables are in the database "
              + copyName()
              + "; see docs/server/upgrades.md (section \"Backup before the upgrade\").";
      case H2 ->
          "The copy is the file "
              + copyName()
              + ". With the server stopped, move it to "
              + h2File("")
              + ".";
    };
  }

  private String quote(String identifier) {
    return kind == DatabaseKind.MYSQL
        ? "`" + identifier.replace("`", "``") + "`"
        : "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
