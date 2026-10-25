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
import java.util.TreeSet;
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
 * up an empty database, adopts a database of the baseline release (recording version 1 as the
 * baseline) and applies the pending steps. On PostgreSQL and MySQL, each {@link #migrate()} holds a
 * {@link RunLock} from classifying the database to the last step.
 */
public final class SchemaMigrator implements AutoCloseable {

  /** What a database holds, compared with the schema steps of this build. */
  enum SchemaState {
    /** No tables, apart from Casbin's authorization rules table. */
    EMPTY,
    /** Every step of this build is applied. */
    CURRENT,
    /** Some steps of this build are not applied yet. */
    PENDING,
    /** A step newer than this build is applied: the database belongs to a newer release. */
    AHEAD,
    /** Unity Catalog's tables without Flyway's history: a database of the baseline release. */
    UNVERSIONED_UC,
    /** Unity Catalog's tables without Flyway's history, lacking some of the baseline's. */
    BEFORE_BASELINE,
    /** Tables, but none of Unity Catalog's. */
    NOT_UC
  }

  /** The databases Unity Catalog's schema steps exist for; each has its folder of steps. */
  enum DatabaseKind {
    H2,
    POSTGRESQL,
    MYSQL;

    /**
     * The database of a JDBC URL, from its prefix.
     *
     * @throws IllegalArgumentException for a URL of any other database
     */
    static DatabaseKind fromJdbcUrl(String url) {
      for (DatabaseKind kind : values()) {
        if (url.startsWith("jdbc:" + kind.folder() + ":")) {
          return kind;
        }
      }
      // Only the scheme: the rest of a URL can hold a password.
      throw new IllegalArgumentException(
          "Unsupported database "
              + url.substring(0, Math.max(url.indexOf(':', "jdbc:".length()), 0))
              + ": use jdbc:h2:, jdbc:postgresql: or jdbc:mysql:");
    }

    /** The folder under {@code db/migration} holding this database's steps. */
    String folder() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);
  private static final String HISTORY_TABLE = "uc_schema_history";

  /**
   * The release whose schema is version 1. A database without schema history from it is adopted as
   * version 1; one from an earlier release has to be started on a {@value} server once first.
   */
  static final String BASELINE_RELEASE = "0.7.0";

  /** The schema version of {@link #BASELINE_RELEASE}, which Flyway records when it adopts one. */
  private static final String BASELINE_VERSION = "1";

  /** The baseline Flyway records on an empty database, below step 1 so that step 1 runs. */
  private static final String EMPTY_BASELINE_VERSION = "0";

  /**
   * The tables of {@link #BASELINE_RELEASE}'s schema. A database without history that lacks one is
   * from an earlier release. Until adoption compares the whole schema with the baseline, only the
   * tables are checked.
   */
  private static final Set<String> BASELINE_TABLES =
      Set.of(
          "uc_catalogs",
          "uc_columns",
          "uc_credentials",
          "uc_delta_commits",
          "uc_dependencies",
          "uc_external_locations",
          "uc_function_params",
          "uc_functions",
          "uc_metastore",
          "uc_model_versions",
          "uc_properties",
          "uc_registered_models",
          "uc_schemas",
          "uc_staging_tables",
          "uc_storage_cleanup_tasks",
          "uc_tables",
          "uc_users",
          "uc_volumes");

  /** The run lock's connection plus Flyway's two (PostgreSQL uses both; MySQL one). */
  private static final int POOL_SIZE = 3;

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final HikariDataSource ownedPool; // null: the caller's pool

  /**
   * A migrator with its own small pool on the database of {@code hibernateProperties}, which {@link
   * #close()} closes. The server's pool can then be any size.
   */
  public static SchemaMigrator fromHibernateProperties(Properties hibernateProperties) {
    HikariConfig config = HibernateConfigurator.hikariConfig(hibernateProperties);
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(0);
    config.setPoolName("schema-migrator");
    HikariDataSource pool = new HikariDataSource(config);
    return new SchemaMigrator(pool, DatabaseKind.fromJdbcUrl(config.getJdbcUrl()), List.of(), pool);
  }

  /** Applies this release's steps to {@code dataSource}; closing it stays with the caller. */
  public SchemaMigrator(DataSource dataSource) {
    this(dataSource, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  SchemaMigrator(DataSource dataSource, List<String> extraLocations) {
    this(dataSource, DatabaseKind.fromJdbcUrl(jdbcUrl(dataSource)), extraLocations, null);
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

  private static String jdbcUrl(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getMetaData().getURL();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to the database: " + e.getMessage(), e);
    }
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
          .baselineDescription(
              EMPTY_BASELINE_VERSION.equals(baselineVersion)
                  ? "empty"
                  : "baseline " + BASELINE_RELEASE);
    }
    return configuration.load();
  }

  /**
   * Under the run lock: classifies the database, then sets it up, adopts it and upgrades it, and
   * checks that it ends at this build's schema version. On PostgreSQL and MySQL, a second run at
   * the same time waits for the first. Returns the number of steps it applied.
   */
  public int migrate() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      SchemaState state = state();
      refuseIfUnsupported(state);
      String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
      int applied = flyway(baselineFor(state)).migrate().migrationsExecuted;
      if (applied > 0) {
        LOGGER.info(
            "Upgraded the database schema from version {} to {} ({} step(s))",
            from == null ? "none" : from,
            currentVersion(),
            applied);
      }
      requireCurrent();
      return applied;
    }
  }

  /**
   * Classifies the database against this build's steps. Changes nothing and does not take the run
   * lock, so a run in progress can change the answer.
   */
  SchemaState state() {
    Set<String> tables = tableNamesLowerCase();
    tables.remove("casbin_rule"); // Casbin's adapter creates it on its own
    if (tables.contains(HISTORY_TABLE)) {
      MigrationInfoService info = flyway(null).info();
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
    if (!tables.contains("uc_catalogs")) {
      return SchemaState.NOT_UC;
    }
    return tables.containsAll(BASELINE_TABLES)
        ? SchemaState.UNVERSIONED_UC
        : SchemaState.BEFORE_BASELINE;
  }

  /** The highest applied version, {@code null} without history. */
  String currentVersion() {
    return version(flyway(null).info().current());
  }

  /** The highest step in this build. */
  String latestVersion() {
    return latestVersion(flyway(null).info());
  }

  private static String latestVersion(MigrationInfoService info) {
    return Arrays.stream(info.all())
        .filter(m -> m.getState().isResolved() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .orElseThrow()
        .getVersion();
  }

  /**
   * The baseline Flyway records before the first step: below step 1 on an empty database so step 1
   * runs, at 1 on a baseline-release database so step 1 is skipped, and none once history exists.
   */
  private static String baselineFor(SchemaState state) {
    return switch (state) {
      case EMPTY -> EMPTY_BASELINE_VERSION;
      case UNVERSIONED_UC -> BASELINE_VERSION;
      default -> null;
    };
  }

  /** Throws for a database that {@link #migrate()} must not change. */
  private void refuseIfUnsupported(SchemaState state) {
    String refusal =
        switch (state) {
          case NOT_UC ->
              "The database has tables but none of Unity Catalog's: not a Unity Catalog database."
                  + " Point the server at an empty database or a Unity Catalog one.";
          case AHEAD ->
              "The schema is newer than this release (version "
                  + currentVersion()
                  + ", this release ends at "
                  + latestVersion()
                  + "). Run the newer release.";
          case BEFORE_BASELINE ->
              "The database is from a release before "
                  + BASELINE_RELEASE
                  + " (missing tables: "
                  + String.join(", ", missingBaselineTables())
                  + "). Start a "
                  + BASELINE_RELEASE
                  + " server on it once, then start this release.";
          default -> null;
        };
    if (refusal != null) {
      throw new IllegalStateException(refusal);
    }
  }

  /** Throws unless the database ends at this build's schema version. */
  private void requireCurrent() {
    SchemaState state = state();
    if (state != SchemaState.CURRENT) {
      throw new IllegalStateException(
          "The database schema is "
              + state
              + " after migration; this release needs version "
              + latestVersion());
    }
  }

  private Set<String> missingBaselineTables() {
    Set<String> missing = new TreeSet<>(BASELINE_TABLES);
    missing.removeAll(tableNamesLowerCase());
    return missing;
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

  /** Closes the migrator's own pool, if it has one. */
  @Override
  public void close() {
    if (ownedPool != null) {
      ownedPool.close();
    }
  }

  /**
   * One lock per database for a whole migration run, so two servers never run the steps at the same
   * time; Flyway's own lock is released between steps. PostgreSQL takes {@code pg_advisory_lock} on
   * a fixed key (advisory locks are per database), MySQL {@code GET_LOCK} on a name derived from
   * the database's (MySQL's locks are per server). The lock is held on one connection until {@link
   * #close()}. H2 has none: its file lock admits one process, and a server runs one migration at
   * start.
   */
  private static final class RunLock implements AutoCloseable {

    /** Any fixed key; Flyway's own advisory lock uses a different one. */
    private static final long POSTGRES_KEY = 0x75635f6d69677261L; // "uc_migra"

    /** The lock name, as SQL; SHA1 keeps it within MySQL's 64-character limit. */
    private static final String MYSQL_NAME = "CONCAT('uc_migrate.', SHA1(DATABASE()))";

    private final Connection connection; // null: no lock (H2)
    private final String unlockSql;

    private RunLock(Connection connection, String unlockSql) {
      this.connection = connection;
      this.unlockSql = unlockSql;
    }

    /**
     * Blocks until this run holds the lock, logging once when another run holds it. Throws when the
     * database fails the lock call or does not grant the lock, so the run never starts unlocked.
     */
    static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
      if (kind == DatabaseKind.H2) {
        return new RunLock(null, null);
      }
      boolean postgres = kind == DatabaseKind.POSTGRESQL;
      String tryLock =
          postgres
              ? "SELECT pg_try_advisory_lock(" + POSTGRES_KEY + ")"
              : "SELECT GET_LOCK(" + MYSQL_NAME + ", 0)";
      String lock =
          postgres
              ? "SELECT true FROM pg_advisory_lock(" + POSTGRES_KEY + ")"
              : "SELECT GET_LOCK(" + MYSQL_NAME + ", -1)";
      String unlock =
          postgres
              ? "SELECT pg_advisory_unlock(" + POSTGRES_KEY + ")"
              : "SELECT RELEASE_LOCK(" + MYSQL_NAME + ")";
      Connection connection = null;
      try {
        connection = dataSource.getConnection();
        // Outside a transaction, so the run doesn't keep one open while it holds the lock.
        connection.setAutoCommit(true);
        if (!isTrue(connection, tryLock)) {
          LOGGER.info("Another server is migrating the schema; waiting for it to finish.");
          if (!isTrue(connection, lock)) {
            throw new SQLException("the database did not grant it");
          }
        }
        return new RunLock(connection, unlock);
      } catch (SQLException e) {
        close(connection);
        throw new IllegalStateException(
            "Could not take the schema migration lock: " + e.getMessage(), e);
      }
    }

    /**
     * Releases the lock. Throws when the release fails, so the server stops instead of starting
     * with the lock state unknown. The database connection underneath is then closed too, which
     * ends its session and so releases the lock; closing a pooled connection only returns it.
     */
    @Override
    public void close() {
      if (connection == null) {
        return;
      }
      try {
        if (!isTrue(connection, unlockSql)) {
          throw new SQLException("the database did not hold it for this run");
        }
      } catch (SQLException e) {
        closeUnderlying(connection);
        throw new IllegalStateException(
            "Could not release the schema migration lock: "
                + e.getMessage()
                + ". The database releases it when the connection closes; start the server again.",
            e);
      } finally {
        close(connection);
      }
    }

    /**
     * Runs a lock call; whether it returned true (MySQL's 1). NULL, MySQL's error result, throws.
     */
    private static boolean isTrue(Connection connection, String sql) throws SQLException {
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        if (!rs.next()) {
          return false;
        }
        boolean value = rs.getBoolean(1);
        if (rs.wasNull()) {
          throw new SQLException("the lock call returned NULL");
        }
        return value;
      }
    }

    private static void closeUnderlying(Connection connection) {
      try {
        connection.unwrap(Connection.class).close();
      } catch (SQLException e) {
        LOGGER.warn(
            "Could not close the schema migration lock's database connection; the lock stays held"
                + " until that database session ends",
            e);
      }
    }

    private static void close(Connection connection) {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOGGER.debug("Could not close the schema migration lock's connection", e);
      }
    }
  }
}
