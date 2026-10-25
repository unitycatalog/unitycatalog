package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.CoreMigrationType;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
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
   * {@code ALTER TABLE t ALTER COLUMN c [SET DATA TYPE | TYPE] type[(n)] [[NOT] NULL]} (H2,
   * PostgreSQL) or {@code ALTER TABLE t MODIFY [COLUMN] c type[(n)] [[NOT] NULL]} (MySQL), and
   * nothing after it: not a type with other parameters, such as MySQL's {@code enum (...)}, nor a
   * statement that changes more than the type, such as with {@code DEFAULT}, {@code COLLATE} or
   * {@code USING}. Group 6 is the {@code NULL} of a nullability, group 5 its {@code NOT}.
   */
  private static final Pattern CHANGE_TYPE =
      Pattern.compile(
          "alter\\s+table\\s+(\\w+)\\s+(?:alter\\s+column|modify(?:\\s+column)?)\\s+(\\w+)\\s+"
              + "(?:set\\s+data\\s+type\\s+|type\\s+)?(\\w+)"
              + "(?:\\s*\\(\\s*(\\d+)\\s*\\))?(?!\\s*\\()"
              + "(?:\\s+(not\\s+)?(null))?\\s*$",
          Pattern.CASE_INSENSITIVE);

  /** The names H2's metadata gives the types the steps name otherwise. */
  private static final Map<String, String> TYPE_NAMES =
      Map.of("clob", "character large object", "varchar", "character varying");

  /** The types the steps name without a size, since they take none. */
  private static final Set<String> UNSIZED_TYPES = Set.of("text", "clob", "mediumtext", "longtext");

  private static final Pattern CHECK = Pattern.compile("C(\\d+(?:_\\d+)*)__(.+)\\.sql");

  /** The most rows of one check that a refusal lists. */
  private static final int MAX_ROWS_LISTED = 20;

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
    locations.add(JAVA_STEPS);
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
    return configuration(baselineVersion).load();
  }

  private FluentConfiguration configuration(String baselineVersion) {
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

  private MigrationOutcome migrateLocked() {
    SchemaState state = state(); // read under the lock
    String from = state == SchemaState.UNVERSIONED_UC ? BASELINE_VERSION : currentVersion();
    String refusal = refusal(state);
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
      boolean alreadyThere = isAlreadyThere(step);
      if (alreadyThere) {
        LOGGER.info(
            "Step {} ({}) is already in the database; recorded it without running it.",
            version(step),
            step.getDescription());
      }
      applied +=
          configuration(baseline)
              .target(step.getVersion())
              .skipExecutingMigrations(alreadyThere)
              .load()
              .migrate()
              .migrationsExecuted;
    }
    return new MigrationOutcome(from, currentVersion(), applied);
  }

  /** The steps above version {@code from} ({@code null}: every step), in order. */
  private List<MigrationInfo> pendingSteps(String from) {
    return Arrays.stream(flyway(null).info().pending())
        .filter(
            m -> from == null || m.getVersion().compareTo(MigrationVersion.fromVersion(from)) > 0)
        .toList();
  }

  /**
   * Whether the change of a SQL step is already in the database, so that running it would fail: a
   * run stopped between the step and its history row, or someone applied it by hand. Recognises a
   * step by its one statement; any other step runs.
   */
  private boolean isAlreadyThere(MigrationInfo step) {
    if (step.getType() != CoreMigrationType.SQL) {
      return false;
    }
    String statement = stepStatement(step);
    if (statement.contains(";") // more than one statement, such as the baseline step
        || hasSeveralChanges(statement)) {
      return false;
    }
    try (Connection connection = dataSource.getConnection()) {
      return isAlreadyThere(statement, connection);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not check whether step "
              + version(step)
              + " is already in the database: "
              + e.getMessage(),
          e);
    }
  }

  private boolean isAlreadyThere(String statement, Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    Matcher createTable = CREATE_TABLE.matcher(statement);
    Matcher addConstraint = ADD_CONSTRAINT.matcher(statement);
    Matcher addColumn = ADD_COLUMN.matcher(statement);
    Matcher createIndex = CREATE_INDEX.matcher(statement);
    Matcher changeType = CHANGE_TYPE.matcher(statement);
    if (createTable.lookingAt()) {
      return tableName(metaData, catalog, schema, createTable.group(1)) != null;
    } else if (addConstraint.lookingAt()) {
      return hasConstraint(connection, addConstraint.group(1), addConstraint.group(2));
    } else if (addColumn.lookingAt()) {
      String table = tableName(metaData, catalog, schema, addColumn.group(1));
      return table != null
          && names(metaData.getColumns(catalog, schema, table, null), "COLUMN_NAME")
              .contains(addColumn.group(2).toLowerCase(Locale.ROOT));
    } else if (createIndex.lookingAt()) {
      String table = tableName(metaData, catalog, schema, createIndex.group(2));
      return table != null
          && names(metaData.getIndexInfo(catalog, schema, table, false, false), "INDEX_NAME")
              .contains(createIndex.group(1).toLowerCase(Locale.ROOT));
    } else if (changeType.lookingAt()) {
      if (changeType.group(4) == null
          && !UNSIZED_TYPES.contains(changeType.group(3).toLowerCase(Locale.ROOT))) {
        return false; // without a size, the type may be a sized one at its default size
      }
      // MySQL's MODIFY and H2's ALTER COLUMN c type (without SET DATA TYPE or TYPE) restate the
      // column: they drop its default, and MySQL's makes it nullable unless it says NOT NULL.
      boolean restates = statement.substring(changeType.end(2), changeType.start(3)).isBlank();
      Boolean nullable =
          changeType.group(6) != null
              ? Boolean.valueOf(changeType.group(5) == null)
              : restates && kind == DatabaseKind.MYSQL ? Boolean.TRUE : null;
      String table = tableName(metaData, catalog, schema, changeType.group(1));
      return table != null
          && hasType(
              metaData.getColumns(catalog, schema, table, null),
              changeType.group(2),
              changeType.group(3),
              changeType.group(4),
              nullable,
              restates);
    }
    return false;
  }

  /**
   * Whether the column {@code column} of the columns {@code rs} (which this closes) has the type
   * {@code type} and, unless {@code null}, the size {@code size} and the nullability {@code
   * nullable}, and when {@code noDefault}, no default.
   */
  private static boolean hasType(
      ResultSet rs, String column, String type, String size, Boolean nullable, boolean noDefault)
      throws SQLException {
    try (rs) {
      while (rs.next()) {
        if (rs.getString("COLUMN_NAME").equalsIgnoreCase(column)) {
          return typeName(rs.getString("TYPE_NAME")).equals(typeName(type))
              && (size == null || rs.getInt("COLUMN_SIZE") == Integer.parseInt(size))
              && (nullable == null || nullable == "YES".equals(rs.getString("IS_NULLABLE")))
              && (!noDefault || rs.getString("COLUMN_DEF") == null);
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
      case FAILED -> {
        MigrationInfo failed =
            Arrays.stream(flyway(null).info().all())
                .filter(m -> m.getState().isFailed())
                .findFirst()
                .orElseThrow();
        yield "A migration step failed in an earlier run: version "
            + version(failed)
            + " ("
            + failed.getDescription()
            + "). See docs/server/upgrades.md.";
      }
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
    try (Connection connection = dataSource.getConnection()) {
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
