package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.CoreMigrationType;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationState;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades Unity Catalog's database schema with the SQL steps of {@code db/migration/<database>}
 * and the Java steps of {@code io.unitycatalog.server.migrate.steps}, which share one sequence of
 * versions: sets up an empty database, adopts one of the baseline release (recording version 1 as
 * the baseline) and applies the pending steps. Each {@link #migrate()} runs under a {@link
 * RunLock}, from classifying the database to the last step.
 *
 * <p>A step that adds a rule existing rows can break ships a check: {@code
 * C<version>__<description>.sql} in {@code db/migration/checks/<database>}, one query that returns
 * the rows breaking it. Before any step runs, the checks of the pending steps run, and the upgrade
 * is refused while one returns rows.
 *
 * <p>A SQL step whose change is already in the database (a run stopped before Flyway recorded it,
 * or someone applied it by hand) is recorded without running it.
 *
 * <p>A step that failed in an earlier run is run again, or recorded when its change is there, if
 * the run can tell what the failed run left: a SQL step whose change is fully there or absent, or a
 * {@link DataStep}, which goes on from the rows it has not changed yet. Otherwise the run refuses.
 *
 * <p>A statement of the run waits at most {@link #LOCK_WAIT} for a lock that another session holds,
 * then fails.
 *
 * <p>When the process is told to stop during the run of a server start, the run stops at the next
 * boundary (before a step, or after a batch of a batched {@link DataStep}) and the next start
 * finishes it.
 */
public final class SchemaMigrator implements AutoCloseable {

  /**
   * The result of {@link #migrate()}.
   *
   * @param from the schema version before the run, {@code null} for an empty database
   * @param to the schema version after the run
   * @param applied the number of steps the run applied, counting those it found already in the
   *     database and recorded without running
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
    /** Unity Catalog's tables without Flyway's history: a database of the baseline release. */
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

    /**
     * The database of a JDBC URL, from its prefix.
     *
     * @throws IllegalArgumentException for a URL of any other database
     */
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
  private static final String HISTORY_TABLE = "uc_schema_history";

  /** The Java steps ({@link DataStep}s), for every database. */
  static final String JAVA_STEPS = "classpath:io/unitycatalog/server/migrate/steps";

  /**
   * The release whose schema is version 1. A database without schema history from it is adopted as
   * version 1; one from an earlier release has to be started on a {@value} server once first.
   */
  public static final String BASELINE_RELEASE = "0.7.0";

  /** The schema version of {@link #BASELINE_RELEASE}, which Flyway records when it adopts one. */
  private static final String BASELINE_VERSION = "1";

  /** The step that creates the schema of {@link #BASELINE_RELEASE}. */
  private static final String BASELINE_STEP = "V1__baseline_0_7.sql";

  private static final Pattern CREATE_TABLE =
      Pattern.compile("create\\s+table\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

  // The statements of the SQL steps whose change can be found in the database.
  private static final Pattern ADD_CONSTRAINT =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+add\\s+constraint\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ADD_COLUMN =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+add\\s+(?:column\\s+)?(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CREATE_INDEX =
      Pattern.compile(
          "create\\s+(?:unique\\s+)?index\\s+(\\w+)\\s+on\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

  /**
   * {@code ALTER TABLE t ALTER COLUMN c [SET DATA TYPE | TYPE] type[(n)]} (H2, PostgreSQL) or
   * {@code ALTER TABLE t MODIFY [COLUMN] c type[(n)]} (MySQL); not a type with other parameters,
   * such as MySQL's {@code enum (...)}.
   */
  private static final Pattern CHANGE_TYPE =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+(?:alter\\s+column|modify(?:\\s+column)?)\\s+(\\w+)\\s+"
              + "(?:set\\s+data\\s+type\\s+|type\\s+)?(\\w+)"
              + "(?:\\s*\\(\\s*(\\d+)\\s*\\))?(?!\\s*\\()",
          Pattern.CASE_INSENSITIVE);

  /** The names H2's metadata gives the types the steps name otherwise. */
  private static final Map<String, String> TYPE_NAMES =
      Map.of("clob", "character large object", "varchar", "character varying");

  /** The types the steps name without a size, since they take none. */
  private static final Set<String> UNSIZED_TYPES = Set.of("text", "clob", "mediumtext", "longtext");

  /** The table a statement waits for: {@code ALTER TABLE t ...} or {@code CREATE INDEX i ON t}. */
  private static final Pattern LOCKED_TABLE =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)|create\\s+(?:unique\\s+)?index\\s+\\w+\\s+on\\s+(\\w+)",
          Pattern.CASE_INSENSITIVE);

  /**
   * How long a statement of a migration run waits for a lock that another session holds before it
   * fails, so a step doesn't wait forever behind a long transaction while the servers are down.
   */
  static final Duration LOCK_WAIT = Duration.ofSeconds(60);

  private static final Pattern CHECK = Pattern.compile("C(\\d+(?:_\\d+)*)__(.+)\\.sql");

  /** The most rows of one check that a refusal lists. */
  private static final int MAX_ROWS_LISTED = 20;

  /** Set by the shutdown hook of {@link #prepareForStart()}: the run stops at its next boundary. */
  private static volatile boolean stopRequested;

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final Duration lockWait;
  private final HikariDataSource ownedPool;

  /** Does not own {@code dataSource}. */
  public SchemaMigrator(DataSource dataSource, DatabaseKind kind) {
    this(dataSource, kind, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  public SchemaMigrator(DataSource dataSource, DatabaseKind kind, List<String> extraLocations) {
    this(dataSource, kind, extraLocations, LOCK_WAIT);
  }

  /** Waits at most {@code lockWait} for a lock instead of {@link #LOCK_WAIT} (tests' own cap). */
  SchemaMigrator(
      DataSource dataSource, DatabaseKind kind, List<String> extraLocations, Duration lockWait) {
    this(dataSource, kind, extraLocations, lockWait, null);
  }

  private SchemaMigrator(
      DataSource dataSource,
      DatabaseKind kind,
      List<String> extraLocations,
      Duration lockWait,
      HikariDataSource ownedPool) {
    this.dataSource = dataSource;
    this.kind = kind;
    List<String> locations = new ArrayList<>();
    locations.add("classpath:db/migration/" + kind.folder());
    locations.add(JAVA_STEPS);
    locations.addAll(extraLocations);
    this.locations = locations.toArray(String[]::new);
    this.lockWait = lockWait;
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
        LOCK_WAIT,
        pool);
  }

  private Flyway flyway(String baselineVersion) { // null: no baselining
    return configuration(baselineVersion).load();
  }

  private FluentConfiguration configuration(String baselineVersion) {
    FluentConfiguration configuration =
        Flyway.configure()
            .dataSource(flywayDataSource())
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
              "0".equals(baselineVersion) ? "empty" : "baseline " + BASELINE_RELEASE);
    }
    return configuration;
  }

  /**
   * Classifies the database against this build's steps. Changes nothing and does not take the run
   * lock, so a run in progress can change the answer.
   */
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

  /** The highest version applied, not counting a failed step; {@code null} without history. */
  public String currentVersion() {
    return Arrays.stream(flyway(null).info().applied())
        .filter(m -> !m.getState().isFailed() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .map(MigrationVersion::getVersion)
        .orElse(null);
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

  /** The entries of the schema history: the baseline, if any, and every step applied or failed. */
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
      MigrationOutcome outcome = migrateStoppingOnShutdown();
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

  /**
   * For {@code bin/start-uc-server --check-schema}: under the run lock, prints the database's
   * state, the pending steps and whether {@link #migrate()} would go through, running the checks of
   * the pending steps. Changes nothing.
   *
   * @return 0 when an upgrade would go through, 1 when {@link #migrate()} would refuse it
   */
  public int checkSchema(PrintStream out) {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      SchemaState state = state();
      String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
      out.println(
          "Database: "
              + state
              + ", schema version "
              + (from == null ? "none" : from)
              + "; this release ends at "
              + latestVersion()
              + ".");
      String refusal = refusal(state);
      if (refusal == null) {
        List<MigrationInfo> pending = pendingSteps(from);
        out.println(pending.isEmpty() ? "Pending steps: none." : "Pending steps:");
        pending.forEach(m -> out.println("  " + version(m) + " " + m.getDescription()));
        refusal = failedChecks(from);
      }
      out.println(refusal == null ? "The upgrade would go through." : refusal);
      return refusal == null ? 0 : 1;
    }
  }

  /**
   * {@link #migrateLocked()} with a shutdown hook that asks the run to stop at its next boundary
   * and waits for it, up to {@link #lockWait}. A statement that is running is not interrupted.
   */
  private MigrationOutcome migrateStoppingOnShutdown() {
    CountDownLatch done = new CountDownLatch(1);
    Thread hook =
        new Thread(
            () -> {
              stopRequested = true;
              try {
                done.await(lockWait.toMillis(), TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "schema-migration-stop");
    Runtime.getRuntime().addShutdownHook(hook);
    try {
      return migrateLocked();
    } finally {
      done.countDown();
      try {
        Runtime.getRuntime().removeShutdownHook(hook);
      } catch (IllegalStateException e) {
        // The JVM is shutting down: the hook is running.
      }
    }
  }

  /**
   * Stops the run at {@code boundary} when the process is shutting down during a server start's
   * run: logs it, also on standard error, and throws.
   */
  static void stopIfRequested(String boundary) {
    if (stopRequested) {
      String message =
          "Stopped the schema migration at "
              + boundary
              + " on SIGTERM; start the server again to finish it.";
      LOGGER.warn(message);
      // Log4j's own shutdown hook has likely stopped the logger by now.
      System.err.println(message);
      throw new IllegalStateException(message);
    }
  }

  private MigrationOutcome migrateLocked() {
    SchemaState state = state(); // read under the lock
    String refusal = refusal(state);
    if (refusal == null && state == SchemaState.FAILED) {
      removeFailedSteps();
      state = state();
      refusal = refusal(state);
    }
    String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
    if (refusal == null) {
      refusal = failedChecks(from);
    }
    if (refusal != null) {
      throw new IllegalStateException(refusal);
    }
    // EMPTY (also with only casbin_rule): baseline at 0 in V1's run, so V1 runs. UNVERSIONED_UC:
    // baseline at 1 before any step: a run that only records steps would record V1 as one of them.
    String baseline = state == SchemaState.EMPTY ? "0" : null;
    if (state == SchemaState.UNVERSIONED_UC) {
      flyway(BASELINE_VERSION).baseline();
    }
    int applied = 0;
    for (MigrationInfo step : pendingSteps(from)) {
      stopIfRequested("the start of step " + version(step) + " (" + step.getDescription() + ")");
      boolean alreadyThere = isAlreadyThere(step);
      if (alreadyThere) {
        LOGGER.info(
            "Step {} ({}) is already in the database; recorded it without running it.",
            version(step),
            step.getDescription());
      }
      try {
        applied +=
            configuration(baseline)
                .target(step.getVersion())
                .skipExecutingMigrations(alreadyThere)
                .load()
                .migrate()
                .migrationsExecuted;
      } catch (FlywayException e) {
        if (isLockWait(e)) {
          throw new IllegalStateException(lockWaitRefusal(step), e);
        }
        throw e;
      }
    }
    return new MigrationOutcome(from, currentVersion(), applied);
  }

  /** The statement that sets {@link #lockWait} on a connection of the run. */
  private String lockWaitSql() {
    return switch (kind) {
      case POSTGRESQL -> "SET lock_timeout = " + lockWait.toMillis();
      // lock_wait_timeout for table (metadata) locks, innodb_lock_wait_timeout for row locks.
      case MYSQL ->
          "SET SESSION lock_wait_timeout = "
              + lockWaitSeconds()
              + ", SESSION innodb_lock_wait_timeout = "
              + lockWaitSeconds();
      case H2 -> "SET LOCK_TIMEOUT " + lockWait.toMillis();
    };
  }

  private long lockWaitSeconds() {
    return Math.max(1, lockWait.toSeconds());
  }

  /**
   * {@link #dataSource} for Flyway, whose connections come from {@link #connection()}. (Flyway's
   * {@code initSql} would do it too, but Flyway logs a deprecation warning for it on every load.)
   */
  private DataSource flywayDataSource() {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection") && args == null) {
                return connection();
              }
              try {
                return method.invoke(dataSource, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  /**
   * A connection of the run, other than the run lock's: Flyway's, the probes' and the checks'. It
   * waits at most {@link #lockWait} for a lock.
   */
  private Connection connection() throws SQLException {
    Connection connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      statement.execute(lockWaitSql());
    } catch (SQLException | RuntimeException e) {
      connection.close();
      throw e;
    }
    return connection;
  }

  /** Whether {@code e} comes from a statement that waited longer than {@link #lockWait}. */
  private boolean isLockWait(Throwable e) {
    for (Throwable cause = e; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql
          && switch (kind) {
            case POSTGRESQL -> "55P03".equals(sql.getSQLState()); // lock_not_available
            case MYSQL -> sql.getErrorCode() == 1205; // ER_LOCK_WAIT_TIMEOUT
            case H2 -> sql.getErrorCode() == 50200; // LOCK_TIMEOUT_1
          }) {
        return true;
      }
    }
    return false;
  }

  /** The refusal for a step that waited longer than {@link #lockWait}, with the table if known. */
  private String lockWaitRefusal(MigrationInfo step) {
    String table = null;
    if (step.getType() == CoreMigrationType.SQL) {
      Matcher matcher = LOCKED_TABLE.matcher(stepStatement(step));
      if (matcher.lookingAt()) {
        table = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
      }
    }
    return "Step "
        + version(step)
        + " ("
        + step.getDescription()
        + ") waited more than "
        + lockWaitSeconds()
        + " seconds for a lock"
        + (table == null ? "" : " on " + table)
        + "; another session holds it. Stop the other sessions on the database, then start the"
        + " server again.";
  }

  /**
   * The steps above version {@code from} ({@code null}: every step) not applied yet or failed in an
   * earlier run, in order.
   */
  private List<MigrationInfo> pendingSteps(String from) {
    return Arrays.stream(flyway(null).info().all())
        .filter(m -> m.getState() == MigrationState.PENDING || m.getState().isFailed())
        .filter(
            m -> from == null || m.getVersion().compareTo(MigrationVersion.fromVersion(from)) > 0)
        .toList();
  }

  /** The steps that failed in an earlier run. */
  private List<MigrationInfo> failedSteps() {
    return Arrays.stream(flyway(null).info().all()).filter(m -> m.getState().isFailed()).toList();
  }

  /**
   * Whether a failed step can run again: a step of this build that is a SQL step whose probe can
   * tell whether its change is there, or a {@link DataStep}, which changes only the rows not
   * changed yet.
   */
  private boolean canRunAgain(MigrationInfo step) {
    if (!step.getState().isResolved()) {
      return false;
    } else if (step.getType() == CoreMigrationType.SQL) {
      return probe(step).isPresent();
    }
    try {
      return DataStep.class.isAssignableFrom(
          Class.forName(step.getScript(), false, SchemaMigrator.class.getClassLoader()));
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Removes the failed steps' history rows with Flyway's repair, so the run runs them again or
   * records them like pending steps.
   */
  private void removeFailedSteps() {
    for (MigrationInfo step : failedSteps()) {
      LOGGER.info(
          "Step {} ({}) failed in an earlier run; running it again.",
          version(step),
          step.getDescription());
    }
    flyway(null).repair();
  }

  /** Whether the change of a SQL step is already in the database; false when it can't tell. */
  private boolean isAlreadyThere(MigrationInfo step) {
    return probe(step).orElse(false);
  }

  /**
   * Whether the change of a SQL step is already in the database, so that running it would fail: a
   * run stopped between the step and its history row, or someone applied it by hand. Recognises a
   * step by its one statement; empty for any other step.
   */
  private Optional<Boolean> probe(MigrationInfo step) {
    if (step.getType() != CoreMigrationType.SQL) {
      return Optional.empty();
    }
    String statement = stepStatement(step);
    if (statement.contains(";") // more than one statement, such as the baseline step
        || hasSeveralChanges(statement)) {
      return Optional.empty();
    }
    try (Connection connection = connection()) {
      return probe(statement, connection);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not check whether step "
              + version(step)
              + " is already in the database: "
              + e.getMessage(),
          e);
    }
  }

  private Optional<Boolean> probe(String statement, Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    Matcher createTable = CREATE_TABLE.matcher(statement);
    Matcher addConstraint = ADD_CONSTRAINT.matcher(statement);
    Matcher addColumn = ADD_COLUMN.matcher(statement);
    Matcher createIndex = CREATE_INDEX.matcher(statement);
    Matcher changeType = CHANGE_TYPE.matcher(statement);
    if (createTable.lookingAt()) {
      return Optional.of(tableName(metaData, catalog, schema, createTable.group(1)) != null);
    } else if (addConstraint.lookingAt()) {
      return Optional.of(hasConstraint(connection, addConstraint.group(1), addConstraint.group(2)));
    } else if (addColumn.lookingAt()) {
      String table = tableName(metaData, catalog, schema, addColumn.group(1));
      return Optional.of(
          table != null
              && names(metaData.getColumns(catalog, schema, table, null), "COLUMN_NAME")
                  .contains(addColumn.group(2).toLowerCase(Locale.ROOT)));
    } else if (createIndex.lookingAt()) {
      String table = tableName(metaData, catalog, schema, createIndex.group(2));
      return Optional.of(
          table != null
              && names(metaData.getIndexInfo(catalog, schema, table, false, false), "INDEX_NAME")
                  .contains(createIndex.group(1).toLowerCase(Locale.ROOT)));
    } else if (changeType.lookingAt()) {
      if (changeType.group(4) == null
          && !UNSIZED_TYPES.contains(changeType.group(3).toLowerCase(Locale.ROOT))) {
        return Optional.empty(); // without a size, the type may be a sized one at its default size
      }
      String table = tableName(metaData, catalog, schema, changeType.group(1));
      return Optional.of(
          table != null
              && hasType(
                  metaData.getColumns(catalog, schema, table, null),
                  changeType.group(2),
                  changeType.group(3),
                  changeType.group(4)));
    }
    return Optional.empty();
  }

  /**
   * Whether the column {@code column} of the columns {@code rs} (which this closes) has the type
   * {@code type} and, unless {@code null}, the size {@code size}.
   */
  private static boolean hasType(ResultSet rs, String column, String type, String size)
      throws SQLException {
    try (rs) {
      while (rs.next()) {
        if (rs.getString("COLUMN_NAME").equalsIgnoreCase(column)) {
          return typeName(rs.getString("TYPE_NAME")).equals(typeName(type))
              && (size == null || rs.getInt("COLUMN_SIZE") == Integer.parseInt(size));
        }
      }
    }
    return false;
  }

  /** A type's name, lower-cased, with H2's names for the short ones the steps use. */
  private static String typeName(String type) {
    String name = type.toLowerCase(Locale.ROOT);
    return TYPE_NAMES.getOrDefault(name, name);
  }

  /**
   * Whether a statement makes several changes, such as {@code ADD COLUMN a INT, ADD COLUMN b INT}:
   * it has a comma outside parentheses and quotes.
   */
  private static boolean hasSeveralChanges(String statement) {
    int depth = 0;
    char quote = 0;
    for (char c : statement.toCharArray()) {
      if (quote != 0) {
        quote = c == quote ? 0 : quote;
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c;
      } else if (c == '(' || c == ')') {
        depth += c == '(' ? 1 : -1;
      } else if (c == ',' && depth == 0) {
        return true;
      }
    }
    return false;
  }

  /** The name of {@code table} as the database stores it, or {@code null} when it is missing. */
  private static String tableName(
      DatabaseMetaData metaData, String catalog, String schema, String table) throws SQLException {
    try (ResultSet rs = metaData.getTables(catalog, schema, null, new String[] {"TABLE"})) {
      while (rs.next()) {
        if (rs.getString("TABLE_NAME").equalsIgnoreCase(table)) {
          return rs.getString("TABLE_NAME");
        }
      }
    }
    return null;
  }

  /** The lower-cased values of column {@code label} of {@code rs}, which this closes. */
  private static Set<String> names(ResultSet rs, String label) throws SQLException {
    Set<String> names = new HashSet<>();
    try (rs) {
      while (rs.next()) {
        if (rs.getString(label) != null) {
          names.add(rs.getString(label).toLowerCase(Locale.ROOT));
        }
      }
    }
    return names;
  }

  /** Whether {@code table} has a key, foreign key or CHECK constraint named {@code constraint}. */
  private boolean hasConstraint(Connection connection, String table, String constraint)
      throws SQLException {
    String sql =
        "SELECT 1 FROM information_schema.table_constraints WHERE table_schema = "
            + (kind == DatabaseKind.MYSQL ? "DATABASE()" : "current_schema")
            + " AND LOWER(table_name) = ? AND LOWER(constraint_name) = ?";
    try (PreparedStatement query = connection.prepareStatement(sql)) {
      query.setString(1, table.toLowerCase(Locale.ROOT));
      query.setString(2, constraint.toLowerCase(Locale.ROOT));
      try (ResultSet rs = query.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** A SQL step's statements without comments or the last semicolon. */
  private String stepStatement(MigrationInfo step) {
    for (String location : locations) {
      if (location.startsWith("classpath:") && !location.equals(JAVA_STEPS)) {
        String folder = location.substring("classpath:".length()).replaceAll("/+$", "");
        InputStream in =
            SchemaMigrator.class
                .getClassLoader()
                .getResourceAsStream(folder + "/" + step.getScript());
        if (in != null) {
          return read(in).replaceAll("--[^\n]*", "").strip().replaceAll(";$", "");
        }
      }
    }
    throw new IllegalStateException("Could not find the step " + step.getScript());
  }

  /**
   * Why {@link #migrate()} refuses a database in {@code state}, or {@code null} when it goes on (a
   * FAILED one when every failed step can run again).
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
      case FAILED ->
          failedSteps().stream()
              .filter(m -> !canRunAgain(m))
              .findFirst()
              .map(
                  m ->
                      "Step "
                          + version(m)
                          + " ("
                          + m.getDescription()
                          + ") failed in an earlier run, and the server can't tell how much of"
                          + " its change is in the database. Restore the backup you took before"
                          + " the upgrade, or undo the step's change by hand and remove its"
                          + " failed record as docs/server/upgrades.md shows (section \"A step"
                          + " that failed\"); then start the server again.")
              .orElse(null);
      case UNVERSIONED_UC -> missingBaselineTables();
      default -> null;
    };
  }

  /**
   * Refuses a database without schema history that lacks a table the baseline step creates: one
   * from a release before {@value #BASELINE_RELEASE}, which this release does not migrate.
   */
  private String missingBaselineTables() {
    Set<String> missing = new TreeSet<>();
    Matcher matcher = CREATE_TABLE.matcher(baselineStep());
    while (matcher.find()) {
      missing.add(matcher.group(1).toLowerCase(Locale.ROOT));
    }
    missing.removeAll(tableNamesLowerCase());
    missing.remove("casbin_rule"); // Casbin's adapter creates it on its own
    if (missing.isEmpty()) {
      return null;
    }
    return "The database is from a release before "
        + BASELINE_RELEASE
        + " (missing tables: "
        + String.join(", ", missing)
        + "). Start a "
        + BASELINE_RELEASE
        + " server on it once, then start this release.";
  }

  /**
   * Runs the check of every step above version {@code from}, each in a read-only transaction of its
   * own, and returns the refusal when any finds rows, or {@code null}. Runs none without a version:
   * an empty database has no rows to check.
   */
  private String failedChecks(String from) {
    if (from == null) {
      return null;
    }
    MigrationVersion current = MigrationVersion.fromVersion(from);
    List<String> failures = new ArrayList<>();
    for (URL check : checks()) {
      MigrationVersion version = checkVersion(check);
      if (version.compareTo(current) > 0) {
        String description = checkName(check).group(2).replace('_', ' ');
        String rows = rowsFound(check, "Check " + version + " (" + description + ")");
        if (rows != null) {
          failures.add(rows);
        }
      }
    }
    if (failures.isEmpty()) {
      return null;
    }
    return "The upgrade is stopped before any change: "
        + failures.size()
        + (failures.size() == 1 ? " check found" : " checks found")
        + " rows that break a rule of this release.\n"
        + String.join("", failures)
        + "Fix them as docs/server/upgrades.md shows (section \"Rows that block an upgrade\"),"
        + " then start the server again.";
  }

  /**
   * The checks of the SQL step locations, by version: those of {@code <base>/<database>} are in
   * {@code <base>/checks/<database>}.
   */
  private List<URL> checks() {
    // By classpath path: like a class, a file on the classpath twice counts once, the first copy.
    Map<String, URL> checks = new LinkedHashMap<>();
    for (String location : locations) {
      if (location.equals(JAVA_STEPS) || !location.startsWith("classpath:")) {
        continue;
      }
      String folder = location.substring("classpath:".length()).replaceAll("/+$", "");
      int slash = folder.lastIndexOf('/');
      String path = folder.substring(0, slash + 1) + "checks/" + folder.substring(slash + 1);
      try {
        for (URL url : Collections.list(SchemaMigrator.class.getClassLoader().getResources(path))) {
          for (String file : fileNames(url, path)) {
            if (CHECK.matcher(file).matches()) {
              checks.putIfAbsent(path + "/" + file, URI.create(url + "/" + file).toURL());
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return checks.values().stream()
        .sorted(Comparator.comparing(SchemaMigrator::checkVersion))
        .toList();
  }

  /** The names of the files in the classpath folder {@code path}, found at {@code url}. */
  private static List<String> fileNames(URL url, String path) throws IOException {
    if (url.getProtocol().equals("jar")) {
      JarURLConnection connection = (JarURLConnection) url.openConnection();
      connection.setUseCaches(false); // a JarFile of its own, to close
      try (JarFile jar = connection.getJarFile()) {
        String prefix = path + "/";
        return jar.stream()
            .map(JarEntry::getName)
            .filter(n -> n.startsWith(prefix) && n.indexOf('/', prefix.length()) < 0)
            .map(n -> n.substring(prefix.length()))
            .toList();
      }
    }
    try (Stream<Path> files = Files.list(Path.of(URI.create(url.toString())))) {
      return files.map(f -> f.getFileName().toString()).toList();
    }
  }

  /** A check's file name matched by {@link #CHECK}: group 1 is the version, 2 the description. */
  private static Matcher checkName(URL check) {
    String path = check.getPath();
    Matcher name = CHECK.matcher(path.substring(path.lastIndexOf('/') + 1));
    name.matches();
    return name;
  }

  /** The version of a check: "C1_1__name.sql" is 1.1. */
  private static MigrationVersion checkVersion(URL check) {
    return MigrationVersion.fromVersion(checkName(check).group(1).replace('_', '.'));
  }

  /**
   * Runs a check in a read-only transaction: the rows it found, at most {@value #MAX_ROWS_LISTED}
   * as {@code column=value}, under {@code title}, or {@code null} when it found none.
   */
  private String rowsFound(URL check, String title) {
    String sql;
    try (InputStream in = check.openStream()) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8).strip().replaceAll(";$", "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    try (Connection connection = connection()) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        StringBuilder rows = new StringBuilder();
        int count = 0;
        while (rs.next()) {
          if (++count <= MAX_ROWS_LISTED) {
            rows.append("  ").append(row(rs)).append('\n');
          }
        }
        if (count > MAX_ROWS_LISTED) {
          rows.append("  ... and ").append(count - MAX_ROWS_LISTED).append(" more\n");
        }
        return count == 0
            ? null
            : title + " found " + count + (count == 1 ? " row:\n" : " rows:\n") + rows;
      } finally {
        connection.rollback();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(title + " failed: " + e.getMessage(), e);
    }
  }

  /** The current row as {@code column=value, ...}, in the query's column order. */
  private static String row(ResultSet rs) throws SQLException {
    ResultSetMetaData columns = rs.getMetaData();
    List<String> values = new ArrayList<>();
    for (int i = 1; i <= columns.getColumnCount(); i++) {
      values.add(columns.getColumnLabel(i).toLowerCase(Locale.ROOT) + "=" + rs.getString(i));
    }
    return String.join(", ", values);
  }

  private String baselineStep() {
    String path = "db/migration/" + kind.folder() + "/" + BASELINE_STEP;
    return read(SchemaMigrator.class.getClassLoader().getResourceAsStream(path));
  }

  /** Reads and closes {@code in}. */
  private static String read(InputStream in) {
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Set<String> tableNamesLowerCase() {
    Set<String> tables = new HashSet<>();
    try (Connection connection = connection();
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
    } else if (dataSource instanceof HikariDataSource pool && pool.getHikariPoolMXBean() != null) {
      // The run's connections keep its lock wait; the pool replaces them with new ones.
      pool.getHikariPoolMXBean().softEvictConnections();
    }
  }

  /**
   * One lock per database for a whole migration run, so two runs never classify or change the
   * schema at the same time. Flyway's own lock is released between steps, so it isn't enough.
   *
   * <ul>
   *   <li>PostgreSQL: {@code pg_advisory_lock} on a fixed key, since PostgreSQL scopes advisory
   *       locks to the database. MySQL: {@code GET_LOCK} on a name derived from the database's.
   *       Either is held on one dedicated connection until {@link #close()}.
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

    /** Blocks until this run holds the lock, logging once when another run holds it. */
    static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
      try {
        if (kind == DatabaseKind.H2) {
          ReentrantLock lock =
              H2_LOCKS.computeIfAbsent(h2Key(dataSource), k -> new ReentrantLock());
          if (!lock.tryLock()) {
            logWaiting();
            lock.lock();
          }
          return new RunLock(kind, null, lock);
        }
        Connection connection = dataSource.getConnection();
        try {
          // Outside a transaction, so the run doesn't keep one open while it holds the lock.
          connection.setAutoCommit(true);
          String trySql =
              kind == DatabaseKind.POSTGRESQL
                  ? "SELECT pg_try_advisory_lock(" + POSTGRES_KEY + ")"
                  : "SELECT GET_LOCK(" + MYSQL_NAME + ", 0)";
          try (Statement statement = connection.createStatement();
              ResultSet rs = statement.executeQuery(trySql)) {
            rs.next();
            if (rs.getBoolean(1)) {
              return new RunLock(kind, connection, null);
            }
          }
          logWaiting();
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

    private static void logWaiting() {
      LOGGER.info("Another server is migrating the schema; waiting for it to finish.");
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
