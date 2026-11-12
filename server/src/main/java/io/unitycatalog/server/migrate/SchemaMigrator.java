package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
 * up an empty database and applies the pending steps to one with schema history. On PostgreSQL and
 * MySQL, each {@link #upgradeSchema()} holds a {@link RunLock} from classifying the database to the
 * last step.
 */
public final class SchemaMigrator implements AutoCloseable {

  /** What a database holds, compared with the schema steps of this build. */
  enum SchemaState {
    /** None of Unity Catalog's tables (names starting with {@code uc_}), but maybe others. */
    EMPTY,
    /** Every step of this build is applied. */
    CURRENT,
    /** Some steps of this build are not applied yet. */
    PENDING,
    /** A step newer than this build is applied: the database belongs to a newer release. */
    AHEAD,
    /**
     * Unity Catalog's tables without Flyway's history, such as a database a server set up when it
     * started.
     */
    UNVERSIONED_UC
  }

  /** The databases Unity Catalog's schema steps exist for; each has its folder of steps. */
  enum DatabaseKind {
    H2,
    POSTGRESQL,
    MYSQL;

    /**
     * The database behind {@code dataSource}, from the name its driver reports. The URL can't tell:
     * a wrapping driver's URL starts with its own prefix ({@code jdbc:aws-wrapper:}, {@code
     * jdbc:p6spy:}), and a pool set up by data source class has none.
     *
     * @throws IllegalArgumentException for any other database
     */
    static DatabaseKind of(DataSource dataSource) {
      String product;
      try (Connection connection = dataSource.getConnection()) {
        product = connection.getMetaData().getDatabaseProductName();
      } catch (SQLException e) {
        throw new IllegalStateException("Could not connect to the database: " + e.getMessage(), e);
      }
      return switch (product) {
        case "H2" -> H2;
        case "PostgreSQL" -> POSTGRESQL;
        case "MySQL" -> MYSQL;
        default ->
            throw new IllegalArgumentException(
                "Unsupported database " + product + ": use H2, PostgreSQL or MySQL");
      };
    }

    /** The folder under {@code db/migration} holding this database's steps. */
    String folder() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);
  private static final String HISTORY_TABLE = "uc_schema_history";

  /**
   * The baseline Flyway records on a database that holds only other tables (Casbin's, another
   * application's), below step 1 so that step 1 runs.
   */
  private static final String EMPTY_BASELINE_VERSION = "0";

  /**
   * The run lock's connection, Flyway's two (PostgreSQL), and one to spare for reading the
   * database's state.
   */
  private static final int POOL_SIZE = 4;

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final HikariDataSource ownedPool; // null: the caller's pool

  /**
   * A migrator with its own small pool on the database of {@code hibernateProperties}, which {@link
   * #close()} closes. The server's pool can then be any size.
   */
  public static SchemaMigrator fromHibernateProperties(Properties hibernateProperties) {
    HibernateConfigurator.requireSchemaFromConnection(hibernateProperties);
    HikariConfig config = HibernateConfigurator.hikariConfig(hibernateProperties);
    refuseH2ServerMode(config);
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(0);
    config.setPoolName("schema-migrator");
    HikariDataSource pool = new HikariDataSource(config);
    try {
      return new SchemaMigrator(pool, DatabaseKind.of(pool), List.of(), pool);
    } catch (RuntimeException e) {
      pool.close();
      throw e;
    }
  }

  /**
   * Runs {@code bin/start-uc-server --upgrade-schema} on the database that the configuration files
   * in {@code configurationFolder} set, printing what it found to {@code out}. Returns the exit
   * code: 0 when the upgrade went through, 1 otherwise, also for a configuration with nothing to
   * upgrade: a missing configuration file or an in-memory database.
   */
  public static int runCommand(Path configurationFolder, PrintStream out) {
    String command = "bin/start-uc-server --upgrade-schema";
    try {
      Path serverFile = configurationFolder.resolve("server.properties");
      Path hibernateFile = configurationFolder.resolve("hibernate.properties");
      for (Path file : List.of(serverFile, hibernateFile)) {
        if (!Files.isRegularFile(file)) {
          throw new IllegalStateException(
              "The configuration file "
                  + file.toAbsolutePath()
                  + " is not found: run "
                  + command
                  + " in the Unity Catalog folder, the one that holds etc/conf.");
        }
      }
      ServerProperties serverProperties = new ServerProperties(serverFile.toString());
      try (SchemaMigrator migrator =
          fromHibernateProperties(
              HibernateConfigurator.setupHibernateProperties(serverProperties, hibernateFile))) {
        if (migrator.isInMemory()) {
          throw new IllegalStateException(
              "The configured database is in memory, so there is nothing to upgrade: the server"
                  + " sets up an in-memory database itself.");
        }
        migrator.upgradeSchema();
        out.println("The database schema is at version " + migrator.latestVersion() + ".");
        return 0;
      }
    } catch (RuntimeException e) {
      LOGGER.error("{} failed", command, e);
      out.println(e.getMessage());
      return 1;
    }
  }

  /**
   * Whether {@code url} is an in-memory H2 URL: {@code jdbc:h2:mem:}, or the unnamed {@code
   * jdbc:h2:.}.
   */
  private static boolean isH2InMemory(String url) {
    return url.startsWith("jdbc:h2:mem:") || url.matches("jdbc:h2:\\.(;.*)?");
  }

  /** Whether the database is an in-memory H2 database, which lasts only as long as its process. */
  public boolean isInMemory() {
    try (Connection connection = dataSource.getConnection()) {
      return isH2InMemory(connection.getMetaData().getURL());
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to the database: " + e.getMessage(), e);
    }
  }

  /**
   * H2 takes no run lock, because it opens a database file in one process. Its server mode lets
   * several processes open the database, so they could run steps twice at once. Read from the
   * configuration, before any connection: the connection's URL leaves {@code AUTO_SERVER} out.
   */
  private static void refuseH2ServerMode(HikariConfig config) {
    Properties driver = config.getDataSourceProperties();
    // Hikari uses the data source class's URL when it has a class; check both URLs.
    for (String url : new String[] {config.getJdbcUrl(), property(driver, "url")}) {
      String lower = url == null ? "" : url.toLowerCase(Locale.ROOT);
      if (lower.contains(":h2:")
          && (lower.contains(":h2:tcp:")
              || lower.contains(":h2:ssl:")
              || isOn(setting(lower, "auto_server"))
              || isOn(property(driver, "auto_server")))) {
        throw new IllegalStateException(
            "H2 in server mode is not supported: the schema migration needs one server per H2"
                + " file. Use a jdbc:h2:file: URL without AUTO_SERVER.");
      }
    }
  }

  /**
   * The value of a {@code ;name=value} setting in a lower-case H2 URL, or {@code null}. Reads the
   * settings as H2 does: a backslash keeps the next character, so {@code auto\_server} is {@code
   * auto_server}.
   */
  private static String setting(String lowerUrl, String name) {
    int start = lowerUrl.indexOf(';');
    if (start < 0) {
      return null;
    }
    List<String> parts = new ArrayList<>();
    StringBuilder part = new StringBuilder();
    for (int i = start + 1; i < lowerUrl.length(); i++) {
      char c = lowerUrl.charAt(i);
      if (c == ';') {
        parts.add(part.toString());
        part.setLength(0);
      } else {
        part.append(c == '\\' && i + 1 < lowerUrl.length() ? lowerUrl.charAt(++i) : c);
      }
    }
    parts.add(part.toString());
    for (String setting : parts) {
      int equals = setting.indexOf('=');
      if (equals > 0 && setting.substring(0, equals).trim().equals(name)) {
        return setting.substring(equals + 1);
      }
    }
    return null;
  }

  /** A driver property whose name matches {@code name} in any case, or {@code null}. */
  private static String property(Properties properties, String name) {
    return properties.stringPropertyNames().stream()
        .filter(key -> key.equalsIgnoreCase(name))
        .map(properties::getProperty)
        .findFirst()
        .orElse(null);
  }

  /** H2 reads a setting as true only for 1, t, y, yes and true (any case); others are off. */
  private static boolean isOn(String value) {
    return value != null
        && Set.of("1", "t", "y", "yes", "true").contains(value.trim().toLowerCase(Locale.ROOT));
  }

  /** Applies this release's steps to {@code dataSource}; closing it stays with the caller. */
  public SchemaMigrator(DataSource dataSource) {
    this(dataSource, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  SchemaMigrator(DataSource dataSource, List<String> extraLocations) {
    this(dataSource, DatabaseKind.of(dataSource), extraLocations, null);
  }

  private SchemaMigrator(
      DataSource dataSource,
      DatabaseKind kind,
      List<String> extraLocations,
      HikariDataSource ownedPool) {
    requireCurrentSchema(dataSource, kind);
    this.dataSource = dataSource;
    this.kind = kind;
    List<String> locations = new ArrayList<>();
    locations.add("classpath:db/migration/" + kind.folder());
    locations.addAll(extraLocations);
    this.locations = locations.toArray(String[]::new);
    this.ownedPool = ownedPool;
  }

  /**
   * On PostgreSQL, a connection whose search_path names no existing schema has no current schema,
   * and a table search without a schema reads every schema.
   */
  private static void requireCurrentSchema(DataSource dataSource, DatabaseKind kind) {
    if (kind != DatabaseKind.POSTGRESQL) {
      return;
    }
    try (Connection connection = dataSource.getConnection()) {
      String schema = connection.getSchema();
      if (schema == null) {
        throw new IllegalStateException(
            "The connection has no current schema: set currentSchema in the JDBC URL to an"
                + " existing schema.");
      }
      // Unqualified names resolve through the whole search_path, so tables that a step creates in
      // a current schema without Unity Catalog tables would hide those in a later schema.
      String later = laterSchemaWithUcTables(connection);
      if (later != null) {
        throw new IllegalStateException(
            "Unity Catalog tables are in schema "
                + later
                + ", which the search_path reaches after the current schema "
                + schema
                + ", so tables that the steps create there would hide them: set currentSchema in"
                + " the JDBC URL to the schema that holds the Unity Catalog tables to use.");
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Could not connect to the database: " + e.getMessage(), e);
    }
  }

  /**
   * When the current schema has no {@code uc_*} tables (its schema history alone does not count), a
   * later schema on the search_path that has them; otherwise {@code null}.
   */
  private static String laterSchemaWithUcTables(Connection connection) throws SQLException {
    String ucTables =
        "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            + " WHERE c.relkind IN ('r', 'p') AND c.relname LIKE 'uc\\_%'";
    try (Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                ucTables
                    + " AND n.nspname = ANY (current_schemas(false))"
                    + " AND n.nspname <> current_schema()"
                    + " AND NOT EXISTS ("
                    + ucTables
                    + " AND n.nspname = current_schema() AND c.relname <> '"
                    + HISTORY_TABLE
                    + "')"
                    + " ORDER BY n.nspname LIMIT 1")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  private FluentConfiguration flyway() {
    return Flyway.configure()
        .dataSource(dataSource)
        .locations(locations)
        .table(HISTORY_TABLE)
        .failOnMissingLocations(true)
        .validateMigrationNaming(true);
  }

  /**
   * For {@code bin/start-uc-server --upgrade-schema}: under the run lock, applies this build's
   * pending steps and checks that the database ends at this build's last step. This also sets up an
   * empty database. A database with Unity Catalog's tables but no schema history is refused. On
   * PostgreSQL and MySQL, a second run at the same time waits for the first. Returns the number of
   * steps it ran.
   */
  public int upgradeSchema() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      refuseIfUnsupported(state());
      requireValidHistory(true);
      int ran =
          flyway()
              .baselineOnMigrate(true)
              .baselineVersion(EMPTY_BASELINE_VERSION)
              .baselineDescription("empty")
              .load()
              .migrate()
              .migrationsExecuted;
      requireCurrent();
      requireStepIndexes();
      return ran;
    }
  }

  /**
   * Throws unless Flyway's validation of the schema history passes: no failed step, no edited step
   * (checksum), no step missing below the recorded version. With {@code upgrading}, steps after the
   * recorded version may be pending: the upgrade runs them.
   */
  private void requireValidHistory(boolean upgrading) {
    if (!tableNamesLowerCase().contains(HISTORY_TABLE)) {
      return;
    }
    FluentConfiguration configuration = flyway();
    if (upgrading) {
      configuration.ignoreMigrationPatterns("*:pending");
    }
    String errors =
        configuration.load().validateWithResult().invalidMigrations.stream()
            .map(m -> m.errorDetails.errorMessage)
            .collect(Collectors.joining("\n"));
    if (!errors.isEmpty()) {
      throw new IllegalStateException(
          "The database's schema history does not match this release's schema steps: " + errors);
    }
  }

  /**
   * Classifies the database against this build's steps. Changes nothing and does not take the run
   * lock, so a run in progress can change the answer. A step that failed counts as not applied.
   */
  SchemaState state() {
    Set<String> tables = tableNamesLowerCase();
    if (tables.contains(HISTORY_TABLE)) {
      String recorded = recordedVersion();
      if (recorded == null) {
        return SchemaState.PENDING;
      }
      int cmp =
          MigrationVersion.fromVersion(recorded)
              .compareTo(MigrationVersion.fromVersion(latestVersion()));
      if (cmp == 0) {
        return SchemaState.CURRENT;
      }
      return cmp > 0 ? SchemaState.AHEAD : SchemaState.PENDING;
    }
    // Other tables (Casbin's, another application's) don't count: the steps run alongside them.
    return tables.stream().anyMatch(t -> t.startsWith("uc_"))
        ? SchemaState.UNVERSIONED_UC
        : SchemaState.EMPTY;
  }

  /**
   * The highest version the schema history records as applied, {@code null} without one. A step
   * that failed does not count; Flyway's own {@code current()} counts it.
   */
  String recordedVersion() {
    if (!tableNamesLowerCase().contains(HISTORY_TABLE)) {
      return null;
    }
    return recordedVersion(flyway().load().info());
  }

  private static String recordedVersion(MigrationInfoService info) {
    return Arrays.stream(info.applied())
        .filter(m -> !m.getState().isFailed() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .map(MigrationVersion::getVersion)
        .orElse(null);
  }

  String latestVersion() {
    List<String> steps = steps();
    return steps.get(steps.size() - 1);
  }

  /** This build's steps, by version, oldest first. */
  List<String> steps() {
    return stepVersions(flyway().load().info());
  }

  private static List<String> stepVersions(MigrationInfoService info) {
    return resolvedSteps(info).stream()
        .map(MigrationInfo::getVersion)
        .sorted()
        .map(MigrationVersion::getVersion)
        .toList();
  }

  /** The versioned steps that this build has. */
  private static List<MigrationInfo> resolvedSteps(MigrationInfoService info) {
    return Arrays.stream(info.all())
        .filter(
            m -> m.getState().isResolved() && m.getVersion() != null && !m.getType().isSynthetic())
        .toList();
  }

  /** Throws for a database that {@link #upgradeSchema()} must not change. */
  private void refuseIfUnsupported(SchemaState state) {
    String refusal =
        switch (state) {
          case AHEAD ->
              "The schema is newer than this release (version "
                  + recordedVersion()
                  + ", this release ends at "
                  + latestVersion()
                  + "). Run the newer release.";
          case UNVERSIONED_UC ->
              "The database has Unity Catalog's tables but no schema history: adopting a database"
                  + " without schema history is not supported yet. Keep running the server on it.";
          default -> null;
        };
    if (refusal != null) {
      throw new IllegalStateException(refusal);
    }
  }

  /**
   * Throws unless the database is at this build's last step: recorded as applied, and Flyway's
   * validation passes.
   */
  private void requireCurrent() {
    SchemaState state = state();
    if (state != SchemaState.CURRENT) {
      throw new IllegalStateException(
          "The database schema is " + state + "; this release needs version " + latestVersion());
    }
    requireValidHistory(false);
  }

  private static final Pattern CREATE_INDEX =
      Pattern.compile(
          "(?i)create\\s+(?:unique\\s+)?index\\s+if\\s+not\\s+exists\\s+(\\w+)\\s+on\\s+(\\w+)");

  /**
   * Throws when an index that a step creates is missing, for example because another object already
   * uses its name. PostgreSQL and H2 name indexes schema-wide, so {@code create index if not
   * exists} skips ours without an error. MySQL names indexes per table and is not affected.
   */
  private void requireStepIndexes() {
    // Flyway's info needs its own connections: list the steps before taking ours.
    List<MigrationInfo> steps = resolvedSteps(flyway().load().info());
    try (Connection connection = dataSource.getConnection()) {
      for (MigrationInfo step : steps) {
        String path = "/db/migration/" + kind.folder() + "/" + step.getScript();
        try (InputStream in = SchemaMigrator.class.getResourceAsStream(path)) {
          if (in == null) {
            continue; // a test's own step
          }
          Matcher m = CREATE_INDEX.matcher(new String(in.readAllBytes(), StandardCharsets.UTF_8));
          while (m.find()) {
            String index = m.group(1);
            String table = m.group(2);
            DatabaseMetaData metaData = connection.getMetaData();
            // H2 stores the unquoted names of the steps in upper case.
            String stored =
                metaData.storesUpperCaseIdentifiers() ? table.toUpperCase(Locale.ROOT) : table;
            if (!indexNames(metaData, connection, stored)
                .contains(index.toLowerCase(Locale.ROOT))) {
              String hint = indexClashHint(connection, index, table);
              throw new IllegalStateException(
                  "Step "
                      + step.getVersion()
                      + " left Unity Catalog index "
                      + index
                      + " missing on "
                      + table
                      + (hint == null ? "" : ": " + hint));
            }
          }
        }
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("Could not check the schema's indexes: " + e.getMessage(), e);
    }
  }

  /**
   * For the index {@code index} missing on {@code table}: when a table that is not Unity Catalog's
   * ({@code uc_*} or {@code os_*}) has an index of that name, the clash: "index idx on other_app,
   * so ... could not create it on uc_x ...". {@code null} when the index is missing for another
   * reason.
   */
  private static String indexClashHint(Connection connection, String index, String table)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    List<String> others = new ArrayList<>();
    try (ResultSet rs =
        metaData.getTables(
            connection.getCatalog(),
            exactPattern(metaData, connection.getSchema()),
            null,
            new String[] {"TABLE"})) {
      while (rs.next()) {
        others.add(rs.getString("TABLE_NAME"));
      }
    }
    for (String other : others) {
      if (!isUnityCatalogTable(other)
          && indexNames(metaData, connection, other).contains(index.toLowerCase(Locale.ROOT))) {
        return "index "
            + index
            + " is used by table "
            + other
            + ", so it could not be created on "
            + table
            + ": restore the backup (on a new install, drop the uc_* tables and uc_schema_history),"
            + " then rename that index or give Unity Catalog its own schema, and run the command"
            + " again";
      }
    }
    return null;
  }

  /** Unity Catalog's tables: {@code uc_*}, and {@code os_*}, which embedded OpenSharing owns. */
  private static boolean isUnityCatalogTable(String table) {
    String lower = table.toLowerCase(Locale.ROOT);
    return lower.startsWith("uc_") || lower.startsWith("os_");
  }

  private static Set<String> indexNames(
      DatabaseMetaData metaData, Connection connection, String table) throws SQLException {
    Set<String> names = new HashSet<>();
    try (ResultSet rs =
        metaData.getIndexInfo(
            connection.getCatalog(), connection.getSchema(), table, false, false)) {
      while (rs.next()) {
        String name = rs.getString("INDEX_NAME");
        if (name != null) {
          names.add(name.toLowerCase(Locale.ROOT));
        }
      }
    }
    return names;
  }

  private Set<String> tableNamesLowerCase() {
    Set<String> tables = new HashSet<>();
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs =
          metaData.getTables(
              connection.getCatalog(),
              exactPattern(metaData, connection.getSchema()),
              null,
              new String[] {"TABLE"})) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME").toLowerCase(Locale.ROOT));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Could not list the database's tables: " + e.getMessage(), e);
    }
    return tables;
  }

  /**
   * {@code name} as a metadata search pattern that matches only itself: {@code _} and {@code %} are
   * wildcards there, so the schema {@code uc_a} would also match {@code ucxa}.
   */
  static String exactPattern(DatabaseMetaData metaData, String name) throws SQLException {
    if (name == null) {
      return null;
    }
    String escape = metaData.getSearchStringEscape();
    return name.replace(escape, escape + escape)
        .replace("_", escape + "_")
        .replace("%", escape + "%");
  }

  @Override
  public void close() {
    if (ownedPool != null) {
      ownedPool.close();
    }
  }

  /**
   * One lock per database for a whole migration run, so two runs never run the steps at the same
   * time; Flyway's own lock is released between steps. PostgreSQL takes {@code pg_advisory_lock} on
   * a fixed key (advisory locks are per database), MySQL {@code GET_LOCK} on a name derived from
   * the database's (MySQL's locks are per server). The lock is held on one connection until {@link
   * #close()}. H2 takes no lock: it locks its database file to one process.
   */
  static final class RunLock implements AutoCloseable {

    /** Any fixed key; Flyway's own advisory lock uses a different one. */
    private static final long POSTGRES_KEY = 0x75635f6d69677261L; // "uc_migra"

    /**
     * The lock name, as SQL. MySQL's locks are per server, so the name includes the database; SHA1
     * keeps it within MySQL's 64-character limit.
     */
    private static final String MYSQL_NAME = "CONCAT('uc_migrate.', SHA1(DATABASE()))";

    /**
     * The lock calls of one database: the try that returns at once, the blocking one, the unlock.
     */
    private record LockCalls(String tryLock, String lock, String unlock) {

      static final LockCalls POSTGRESQL =
          new LockCalls(
              "SELECT pg_try_advisory_lock(" + POSTGRES_KEY + ")",
              "SELECT true FROM pg_advisory_lock(" + POSTGRES_KEY + ")",
              "SELECT pg_advisory_unlock(" + POSTGRES_KEY + ")");

      static final LockCalls MYSQL =
          new LockCalls(
              "SELECT GET_LOCK(" + MYSQL_NAME + ", 0)",
              "SELECT GET_LOCK(" + MYSQL_NAME + ", -1)",
              "SELECT RELEASE_LOCK(" + MYSQL_NAME + ")");
    }

    private final Connection connection; // null on H2
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
      LockCalls calls = kind == DatabaseKind.POSTGRESQL ? LockCalls.POSTGRESQL : LockCalls.MYSQL;
      Connection connection = null;
      try {
        connection = dataSource.getConnection();
        // Outside a transaction, so the run doesn't keep one open while it holds the lock.
        connection.setAutoCommit(true);
        if (!isTrue(connection, calls.tryLock())) {
          logWaiting();
          if (!isTrue(connection, calls.lock())) {
            throw new SQLException("the database did not grant it");
          }
        }
        return new RunLock(connection, calls.unlock());
      } catch (SQLException e) {
        close(connection);
        throw new IllegalStateException(
            "Could not take the schema migration lock: " + e.getMessage(), e);
      }
    }

    /**
     * Releases the lock. Throws when the release fails, so the command stops instead of finishing
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
                + ". The database releases it when the connection closes; run the command again.",
            e);
      } finally {
        close(connection);
      }
    }

    private static void logWaiting() {
      LOGGER.info("Another run is migrating the schema; waiting for it to finish.");
    }

    /**
     * Runs a lock call and returns whether it returned true (MySQL's 1). A NULL result throws
     * (MySQL's error result); a false result returns false.
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
