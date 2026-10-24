package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades Unity Catalog's database schema with the steps of {@code db/migration/<database>}: sets
 * up an empty database, adopts a 0.7 one (recording version 1 as the baseline) and applies the
 * pending steps. Each {@link #migrate()} runs under a {@link RunLock}, from classifying the
 * database to the last step.
 */
public final class SchemaMigrator implements AutoCloseable {

  /**
   * The result of {@link #migrate()}.
   *
   * @param from the schema version before the run, {@code null} for an empty database
   * @param to the schema version after the run
   * @param applied the number of steps the run applied
   */
  public record MigrationOutcome(String from, String to, int applied) {}

  /** What a database holds, compared with the schema steps of this build. */
  public enum SchemaState {
    /** No tables, apart from Casbin's authorization rules table. */
    EMPTY,
    /** Every step of this build is applied. */
    CURRENT,
    /** Some steps of this build are not applied yet. */
    PENDING,
    /** A step newer than this build is applied: the database belongs to a newer release. */
    AHEAD,
    /** Unity Catalog's tables without Flyway's history: a database from 0.7. */
    UNVERSIONED_UC,
    /** Tables, but none of Unity Catalog's. */
    NOT_UC,
    /** An earlier run recorded a failed step. */
    FAILED
  }

  /** The databases Unity Catalog's schema steps exist for. */
  public enum DatabaseKind {
    H2("h2"),
    POSTGRESQL("postgresql"),
    MYSQL("mysql");

    private final String folder;

    DatabaseKind(String folder) {
      this.folder = folder;
    }

    public static DatabaseKind fromJdbcUrl(String url) {
      if (url.startsWith("jdbc:h2:")) {
        return H2;
      } else if (url.startsWith("jdbc:postgresql:")) {
        return POSTGRESQL;
      } else if (url.startsWith("jdbc:mysql:")) {
        return MYSQL;
      }
      throw new IllegalArgumentException(
          "Unsupported database URL " + url + ": use jdbc:h2:, jdbc:postgresql: or jdbc:mysql:");
    }

    /** The folder under {@code db/migration} holding this database's steps. */
    public String folder() {
      return folder;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);
  private static final String HISTORY_TABLE = "flyway_schema_history";

  /** The version of the 0.7 schema, which Flyway records when it adopts a 0.7 database. */
  private static final String BASELINE_0_7 = "1";

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final HikariDataSource ownedPool;

  /** Does not own {@code dataSource}. */
  public SchemaMigrator(DataSource dataSource, DatabaseKind kind) {
    this(dataSource, kind, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  public SchemaMigrator(DataSource dataSource, DatabaseKind kind, List<String> extraLocations) {
    this(dataSource, kind, extraLocations, null);
  }

  private SchemaMigrator(
      DataSource dataSource,
      DatabaseKind kind,
      List<String> extraLocations,
      HikariDataSource ownedPool) {
    this.dataSource = dataSource;
    this.kind = kind;
    List<String> locations = new ArrayList<>();
    locations.add("classpath:db/migration/" + kind.folder());
    locations.addAll(extraLocations);
    this.locations = locations.toArray(String[]::new);
    this.ownedPool = ownedPool;
  }

  /**
   * A migrator on the database of the server's Hibernate properties, with its own small pool that
   * {@link #close()} closes.
   */
  public static SchemaMigrator fromHibernateProperties(Properties hibernateProperties) {
    HikariConfig config = HibernateConfigurator.hikariConfig(hibernateProperties);
    // The run lock's connection plus Flyway's two.
    config.setMaximumPoolSize(3);
    config.setMinimumIdle(0);
    config.setPoolName("schema-migrator");
    HikariDataSource pool = new HikariDataSource(config);
    return new SchemaMigrator(
        pool,
        DatabaseKind.fromJdbcUrl(hibernateProperties.getProperty("hibernate.connection.url")),
        List.of(),
        pool);
  }

  private Flyway flyway(String baselineVersion) { // null: no baselining
    FluentConfiguration configuration =
        Flyway.configure()
            .dataSource(dataSource)
            .locations(locations)
            .table(HISTORY_TABLE)
            .failOnMissingLocations(true)
            .cleanDisabled(true)
            .validateMigrationNaming(true);
    if (baselineVersion != null) {
      configuration
          .baselineOnMigrate(true)
          .baselineVersion(baselineVersion)
          .baselineDescription("0".equals(baselineVersion) ? "empty" : "baseline 0.7");
    }
    return configuration.load();
  }

  public SchemaState state() {
    Set<String> tables = tableNamesLowerCase();
    tables.remove("casbin_rule"); // Casbin's adapter creates it on its own
    if (tables.contains(HISTORY_TABLE)) {
      MigrationInfoService info = flyway(null).info();
      if (Arrays.stream(info.all()).anyMatch(m -> m.getState().isFailed())) {
        return SchemaState.FAILED;
      }
      String current = version(info.current());
      if (current == null) {
        return SchemaState.PENDING;
      }
      int cmp =
          MigrationVersion.fromVersion(current)
              .compareTo(MigrationVersion.fromVersion(latestVersion(info)));
      return cmp == 0 ? SchemaState.CURRENT : cmp > 0 ? SchemaState.AHEAD : SchemaState.PENDING;
    }
    if (tables.isEmpty()) {
      return SchemaState.EMPTY;
    }
    return tables.contains("uc_catalogs") ? SchemaState.UNVERSIONED_UC : SchemaState.NOT_UC;
  }

  /** The highest applied version, {@code null} without history. */
  public String currentVersion() {
    return version(flyway(null).info().current());
  }

  /** The highest step in this build. */
  public String latestVersion() {
    return latestVersion(flyway(null).info());
  }

  private String latestVersion(MigrationInfoService info) {
    return Arrays.stream(info.all())
        .filter(m -> m.getState().isResolved() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .map(MigrationVersion::getVersion)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No schema steps found in "
                        + String.join(", ", locations)
                        + ": the server jar is incomplete"));
  }

  public List<MigrationInfo> applied() {
    return List.of(flyway(null).info().applied());
  }

  /** Under the run lock: classifies the database, then sets it up, adopts it and upgrades it. */
  public MigrationOutcome migrate() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      return migrateLocked();
    }
  }

  /**
   * Run by every server start: under the run lock, migrates the database like {@link #migrate()}
   * and returns the state after. A second server starting at the same time waits for the first.
   */
  public SchemaState prepareForStart() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      MigrationOutcome outcome = migrateLocked();
      if (outcome.applied() > 0) {
        LOGGER.info(
            "Upgraded the database schema from version {} to {} ({} steps)",
            outcome.from() == null ? "none" : outcome.from(),
            outcome.to(),
            outcome.applied());
      }
      return state();
    }
  }

  private MigrationOutcome migrateLocked() {
    SchemaState state = state(); // read under the lock
    String refusal = refusal(state);
    if (refusal != null) {
      throw new IllegalStateException(refusal);
    }
    String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_0_7 : currentVersion();
    // EMPTY (also with only casbin_rule): baseline at 0 so V1 runs; UNVERSIONED_UC: baseline at 1.
    String baseline =
        state == SchemaState.EMPTY
            ? "0"
            : state == SchemaState.UNVERSIONED_UC ? BASELINE_0_7 : null;
    int applied = flyway(baseline).migrate().migrationsExecuted;
    return new MigrationOutcome(from, currentVersion(), applied);
  }

  /**
   * Why {@link #migrate()} refuses a database in {@code state}, or {@code null} when it goes on.
   */
  private String refusal(SchemaState state) {
    return switch (state) {
      case NOT_UC ->
          "The database has tables but none of Unity Catalog's: not a Unity Catalog database."
              + " Point the server at an empty database or a Unity Catalog one.";
      case AHEAD ->
          "The schema is newer than this release (version "
              + currentVersion()
              + ", this release ends at "
              + latestVersion()
              + "). Run the newer release.";
      case FAILED -> "A migration step failed in an earlier run. See docs/server/upgrades.md.";
      default -> null;
    };
  }

  private Set<String> tableNamesLowerCase() {
    Set<String> tables = new HashSet<>();
    try (Connection connection = dataSource.getConnection();
        ResultSet rs =
            connection
                .getMetaData()
                .getTables(
                    connection.getCatalog(),
                    connection.getSchema(),
                    null,
                    new String[] {"TABLE"})) {
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME").toLowerCase(Locale.ROOT));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Could not list the database's tables: " + e.getMessage(), e);
    }
    return tables;
  }

  private static String version(MigrationInfo info) {
    return info == null || info.getVersion() == null ? null : info.getVersion().getVersion();
  }

  @Override
  public void close() {
    if (ownedPool != null) {
      ownedPool.close();
    }
  }

  /**
   * One lock per database for a whole migration run, so two runs never classify or change the
   * schema at the same time. Flyway's own lock is released between steps, so it isn't enough.
   *
   * <ul>
   *   <li>PostgreSQL: {@code pg_advisory_lock} and MySQL: {@code GET_LOCK} on a name derived from
   *       the database's, held on one dedicated connection until {@link #close()}.
   *   <li>H2: an embedded H2 file database is opened by one process, so a lock inside this JVM,
   *       keyed by the database, is enough.
   * </ul>
   */
  private static final class RunLock implements AutoCloseable {

    /** Any fixed key; Flyway's own advisory lock uses a different one. */
    private static final long POSTGRES_KEY = 0x75635f6d69677261L; // "uc_migra"

    /**
     * The lock name, as SQL. MySQL's locks are global to the server, so the name includes the
     * database; SHA1 keeps it within MySQL's 64-character limit.
     */
    private static final String MYSQL_NAME = "CONCAT('uc_migrate.', SHA1(DATABASE()))";

    private static final ConcurrentMap<String, ReentrantLock> H2_LOCKS = new ConcurrentHashMap<>();

    private final DatabaseKind kind;
    private final Connection connection; // PostgreSQL and MySQL
    private final ReentrantLock h2Lock; // H2

    private RunLock(DatabaseKind kind, Connection connection, ReentrantLock h2Lock) {
      this.kind = kind;
      this.connection = connection;
      this.h2Lock = h2Lock;
    }

    /** Blocks until this run holds the lock. */
    static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
      try {
        if (kind == DatabaseKind.H2) {
          ReentrantLock lock =
              H2_LOCKS.computeIfAbsent(h2Key(dataSource), k -> new ReentrantLock());
          lock.lock();
          return new RunLock(kind, null, lock);
        }
        Connection connection = dataSource.getConnection();
        try {
          // Outside a transaction, so the run doesn't keep one open while it holds the lock.
          connection.setAutoCommit(true);
          String sql =
              kind == DatabaseKind.POSTGRESQL
                  ? "SELECT pg_advisory_lock(" + POSTGRES_KEY + ")"
                  : "SELECT GET_LOCK(" + MYSQL_NAME + ", -1)";
          try (Statement statement = connection.createStatement();
              ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            if (kind == DatabaseKind.MYSQL && rs.getInt(1) != 1) {
              throw new IllegalStateException("MySQL did not grant the schema migration lock");
            }
          }
          return new RunLock(kind, connection, null);
        } catch (SQLException | RuntimeException e) {
          connection.close();
          throw e;
        }
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Could not take the schema migration lock: " + e.getMessage(), e);
      }
    }

    /** The database file's path, or the name of an in-memory database. */
    private static String h2Key(DataSource dataSource) throws SQLException {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery("SELECT COALESCE(DATABASE_PATH(), DATABASE())")) {
        rs.next();
        return rs.getString(1);
      }
    }

    @Override
    public void close() {
      if (h2Lock != null) {
        h2Lock.unlock();
        return;
      }
      String sql =
          kind == DatabaseKind.POSTGRESQL
              ? "SELECT pg_advisory_unlock(" + POSTGRES_KEY + ")"
              : "SELECT RELEASE_LOCK(" + MYSQL_NAME + ")";
      try (Connection c = connection;
          Statement statement = c.createStatement()) {
        statement.execute(sql);
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Could not release the schema migration lock: " + e.getMessage(), e);
      }
    }
  }
}
