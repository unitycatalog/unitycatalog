package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.CoreErrorCode;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades Unity Catalog's database schema with the steps of {@code db/migration/<database>}: sets
 * up an empty database, adopts a database of the baseline release (recording step 1 without running
 * it) or of this build's last step (recording every step without running it) and applies the
 * pending steps, checking the schema against the steps' effect files ({@code
 * schema-effects/<database>}) before each step and at the end. On PostgreSQL and MySQL, each {@link
 * #upgradeSchema()} holds a {@link RunLock} from classifying the database to the last step.
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

  private static final Pattern CREATE_INDEX =
      Pattern.compile(
          "(?i)create\\s+(?:unique\\s+)?index\\s+(?:if\\s+not\\s+exists\\s+)?(\\w+)\\s+on\\s+(\\w+)");

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);
  private static final String HISTORY_TABLE = "uc_schema_history";

  /**
   * The release whose databases are adopted as version 1: one without schema history whose schema
   * is step 1's exactly is recorded at version 1 without running step 1. A database from an earlier
   * release has to be started on a {@value} server once first.
   */
  static final String ADOPTED_RELEASE = "0.7.0";

  /**
   * The baseline Flyway records on a database that holds only other tables (Casbin's, another
   * application's), below step 1 so that step 1 runs; also the version before the first step.
   */
  private static final String EMPTY_BASELINE_VERSION = "0";

  /**
   * The run lock's connection, Flyway's two (PostgreSQL), and one to spare for reading the schema
   * between steps.
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
   * Runs {@code bin/start-uc-server --check-schema} (when {@code upgrade} is false) or {@code
   * --upgrade-schema} on the database that the configuration files in {@code configurationFolder}
   * set, printing what it found to {@code out}. Returns the exit code: 0 when the check or the
   * upgrade went through, 1 otherwise, also for a configuration with nothing to upgrade: a missing
   * configuration file, an in-memory database, or Hibernate managing the schema.
   */
  public static int runCommand(boolean upgrade, Path configurationFolder, PrintStream out) {
    String command = "bin/start-uc-server " + (upgrade ? "--upgrade-schema" : "--check-schema");
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
      if (serverProperties.isSchemaManagedByHibernate()) {
        throw new IllegalStateException(
            "server.schema-management=hibernate: Hibernate manages the schema, and the schema"
                + " steps do not run. Set server.schema-management=flyway first; see"
                + " docs/server/upgrades.md.");
      }
      Properties hibernateProperties =
          HibernateConfigurator.setupHibernateProperties(serverProperties, hibernateFile);
      if (!upgrade) {
        openH2OnlyIfItExists(hibernateProperties);
      }
      try (SchemaMigrator migrator = fromHibernateProperties(hibernateProperties)) {
        if (migrator.isInMemory()) {
          throw new IllegalStateException(
              "The configured database is in memory, so there is nothing to check or upgrade: the"
                  + " server sets up an in-memory database itself.");
        }
        if (!upgrade) {
          return migrator.checkSchema(out);
        }
        migrator.upgradeSchema();
        out.println("The database schema is at version " + migrator.latestVersion() + ".");
        return 0;
      }
    } catch (RuntimeException e) {
      LOGGER.error("{} failed", command, e);
      out.println(
          !upgrade && isH2DatabaseNotFound(e)
              ? "The H2 database file that hibernate.connection.url names is not found, and a"
                  + " check does not create it: check the configuration, and run "
                  + command
                  + " in the Unity Catalog folder, the one that holds etc/conf."
              : e.getMessage());
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

  /**
   * Makes a check open an H2 file database only if it exists: H2 creates a missing file when it
   * connects, and a check must change nothing. Sets {@code IFEXISTS=TRUE} on each H2 file URL the
   * pool may use ({@code hibernate.connection.url}, or the data source class's URL property),
   * replacing an {@code IFEXISTS} the URL sets. Returns whether there was one.
   */
  private static boolean openH2OnlyIfItExists(Properties hibernateProperties) {
    boolean h2File = false;
    for (String key : hibernateProperties.stringPropertyNames()) {
      boolean urlSetting =
          key.equals("hibernate.connection.url")
              || (key.startsWith(HIKARI_DATA_SOURCE)
                  && key.substring(HIKARI_DATA_SOURCE.length()).equalsIgnoreCase("url"));
      String url = hibernateProperties.getProperty(key);
      String lower = url.toLowerCase(Locale.ROOT);
      if (urlSetting && lower.startsWith("jdbc:h2:") && !isH2InMemory(lower)) {
        hibernateProperties.setProperty(
            key, url.replaceAll("(?i);\\s*ifexists\\s*=[^;]*", "") + ";IFEXISTS=TRUE");
        h2File = true;
      }
    }
    return h2File;
  }

  private static final String HIKARI_DATA_SOURCE = "hibernate.hikari.dataSource.";

  /** Whether H2 failed to open {@code e}'s database because it does not exist. */
  private static boolean isH2DatabaseNotFound(Throwable e) {
    for (Throwable cause = e; cause != null; cause = cause.getCause()) {
      // 90013: database not found; 90146: not found with IFEXISTS=TRUE
      if (cause instanceof SQLException sql
          && (sql.getErrorCode() == 90013 || sql.getErrorCode() == 90146)) {
        return true;
      }
    }
    return false;
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
   * H2's run lock holds only within this process, because it opens a database file in one process.
   * Its server mode lets several processes open the database, so they could run steps twice at
   * once. Read from the configuration, before any connection: the connection's URL leaves {@code
   * AUTO_SERVER} out.
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

  /** This build's Flyway configuration; tests add callbacks to it. */
  FluentConfiguration flyway() {
    return Flyway.configure()
        .dataSource(dataSource)
        .locations(locations)
        .table(HISTORY_TABLE)
        .failOnMissingLocations(true)
        .validateMigrationNaming(true);
  }

  /**
   * For {@code bin/start-uc-server --upgrade-schema}: under the run lock, brings the database to
   * this build's schema one step at a time, and checks that it ends with the schema of the last
   * step. Before each pending step the whole schema is compared with the steps' effect files
   * ({@code schema-effects/<database>}): it must be the schema before the step, so the step runs,
   * or after it (a run stopped before Flyway recorded the step) or after this build's last step (an
   * adoption that stopped part way), so the step is recorded without running; otherwise the run
   * stops, changing nothing more. Once the schema check accepts a step, its record as a failed step
   * (MySQL and H2 keep one) is deleted, so that the step is tried again. This also sets up an empty
   * database, and adopts a database without schema history when its schema is exactly that of
   * {@link #ADOPTED_RELEASE} (the steps after the first run) or of this build's last step (every
   * step is recorded without running). A second run at the same time waits for the first: on H2,
   * one in this process. Returns the number of steps it ran or recorded.
   */
  public int upgradeSchema() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      refuseIfUnsupported(state());
      requireValidHistory(true);
      boolean adopted = hasLastStepsSchemaWithoutHistory();
      int changed = 0;
      for (String step : pendingSteps()) {
        SchemaSnapshot schema = SchemaSnapshot.of(dataSource);
        boolean record = adopted || !runs(step, schema);
        // Only once the schema check passed, so that a run that refuses leaves the history as is.
        deleteFailedRecord(step);
        MigrateResult result =
            flyway()
                .baselineOnMigrate(true)
                .baselineVersion(EMPTY_BASELINE_VERSION)
                .baselineDescription("empty")
                .target(step)
                .skipExecutingMigrations(record)
                .load()
                .migrate();
        if (result.migrationsExecuted != 1 || !step.equals(recordedVersion())) {
          throw new IllegalStateException(
              "Schema step "
                  + step
                  + " was not recorded as the only change of its run. Run"
                  + " bin/start-uc-server --check-schema to see the database's state.");
        }
        LOGGER.info(
            record
                ? "Recorded schema step {} without running it: the database already has its change"
                : "Ran schema step {}",
            step);
        changed++;
      }
      requireCurrent();
      return changed;
    }
  }

  /**
   * For {@code bin/start-uc-server --check-schema}: prints the database's schema version and what
   * {@link #upgradeSchema()} would do with each pending step, and whether the schema allows the
   * upgrade, running the same checks on the schema as it would. It does not test the database
   * user's privileges or the run lock. Changes nothing and takes no run lock, so it can run while
   * servers of the previous release still serve. A step of this build that failed counts as
   * pending, since the upgrade deletes its record once the step's schema check passes.
   *
   * @return 0 when the schema allows the upgrade, 1 when it would refuse
   */
  int checkSchema(PrintStream out) {
    try {
      refuseIfUnsupported(state());
      requireValidHistory(true);
      String recorded = recordedVersion();
      out.println(
          "Database schema version: "
              + (recorded == null ? "none" : recorded)
              + "; this release ends at "
              + latestVersion()
              + ".");
      List<String> pending = pendingSteps();
      out.println(pending.isEmpty() ? "Pending steps: none." : "Pending steps:");
      Map<String, String> descriptions = descriptions();
      SchemaSnapshot schema = SchemaSnapshot.of(dataSource);
      boolean adopted = hasLastStepsSchemaWithoutHistory();
      for (String step : pending) {
        boolean runs = !adopted && runs(step, schema);
        out.println(
            "  "
                + step
                + " "
                + descriptions.get(step)
                + (runs ? ": runs" : ": recorded without running (its change is already there)"));
        if (runs) {
          schema = expected(step);
        }
      }
      SchemaSnapshot expected = expected(latestVersion());
      if (!schema.equals(expected)) {
        throw new IllegalStateException(
            "The database schema differs from the schema of version "
                + latestVersion()
                + ":\n"
                + differences(schema, expected));
      }
      out.println("The schema allows the upgrade: --upgrade-schema would run the steps above.");
      return 0;
    } catch (IllegalStateException e) {
      out.println(e.getMessage());
      out.println("The schema does not allow the upgrade.");
      return 1;
    }
  }

  private Map<String, String> descriptions() {
    Map<String, String> descriptions = new HashMap<>();
    for (MigrationInfo m : resolvedSteps(flyway().load().info())) {
      descriptions.put(m.getVersion().getVersion(), m.getDescription());
    }
    return descriptions;
  }

  /**
   * For server start, which changes nothing: throws unless the database is at this build's last
   * step, Flyway's validation passes (no failed step, edited step or step missing below the
   * recorded version), and the schema is the one the steps' effect files add up to. The message
   * names what brings the database there. Takes no run lock.
   */
  public void requireCurrentSchema() {
    SchemaState state = state();
    refuseIfUnsupported(state);
    String upgrade =
        "Stop every server of the previous release, run bin/start-uc-server --upgrade-schema with"
            + " this release, then start the server.";
    if (state == SchemaState.EMPTY) {
      throw new IllegalStateException(
          "The database has no Unity Catalog schema yet. Set it up with bin/start-uc-server"
              + " --upgrade-schema, then start the server.");
    }
    if (state != SchemaState.CURRENT) {
      String recorded = recordedVersion();
      throw new IllegalStateException(
          "The database schema is at version "
              + (recorded == null ? "none" : recorded)
              + ", and this release needs version "
              + latestVersion()
              + ". "
              + upgrade);
    }
    try {
      requireValidHistory(false);
    } catch (IllegalStateException e) {
      throw new IllegalStateException(
          e.getMessage()
              + ". Run bin/start-uc-server --check-schema to see what an upgrade would do; "
              + upgrade,
          e);
    }
    requireLastStepsSchema(", then start the server");
  }

  /**
   * For server start, before Hibernate connects, which creates a missing H2 database file: throws
   * when {@code hibernateProperties} names an H2 database file that does not exist, so that a
   * mistyped path is not taken for a new database.
   */
  public static void requireH2DatabaseExists(Properties hibernateProperties) {
    Properties properties = new Properties();
    properties.putAll(hibernateProperties);
    if (!openH2OnlyIfItExists(properties)) {
      return;
    }
    try (SchemaMigrator ignored = fromHibernateProperties(properties)) {
      // Opening it is the check.
    } catch (RuntimeException e) {
      if (isH2DatabaseNotFound(e)) {
        throw new IllegalStateException(
            "The H2 database file that hibernate.connection.url names is not found. Check the"
                + " configuration; to set up a new database there, run bin/start-uc-server"
                + " --upgrade-schema, then start the server.",
            e);
      }
      throw e;
    }
  }

  /**
   * Whether pending {@code step} runs on a database with {@code schema}: true when the schema is
   * the one before the step, false when it already has the step's change or is the schema of this
   * build's last step (an adoption that stopped after recording some steps), and for a first step
   * whose earlier run stopped part way (the schema history exists and holds part of its schema)
   * true, since the first step skips what is already there.
   *
   * @throws IllegalStateException when the schema is neither, naming the differences
   */
  private boolean runs(String step, SchemaSnapshot schema) {
    String previous = previousStep(step);
    SchemaSnapshot before = expected(previous);
    SchemaSnapshot after = expected(step);
    if (schema.equals(before)) {
      return true;
    }
    if (schema.equals(after) || schema.equals(expected(latestVersion()))) {
      return false;
    }
    if (previous.equals(EMPTY_BASELINE_VERSION)
        && tableNamesLowerCase().contains(HISTORY_TABLE)
        && isLeftByAStoppedRun(step, schema, after)) {
      return true;
    }
    String nearer =
        schema.differencesFrom(before).lines().count()
                <= schema.differencesFrom(after).lines().count()
            ? previous
            : step;
    throw new IllegalStateException(
        "The database schema is neither the schema of version "
            + previous
            + " nor that of version "
            + step
            + ", so schema step "
            + step
            + " can't run. It differs from version "
            + nearer
            + " in:\n"
            + differences(schema, expected(nearer))
            + "\nRepair these differences (see docs/server/upgrades.md) or restore the backup,"
            + " then run bin/start-uc-server --upgrade-schema again."
            + (tableNamesLowerCase().contains(HISTORY_TABLE)
                ? ""
                : " A database from a release before "
                    + ADOPTED_RELEASE
                    + " must be started once on a "
                    + ADOPTED_RELEASE
                    + " server first (see docs/server/upgrades.md, \"Repairing a database from"
                    + " before 0.7.0\")."));
  }

  /**
   * Whether {@code schema} is what a run of first step {@code step} that stopped part way leaves,
   * and a rerun completes into {@code after}: some of its tables, each whole except for the indexes
   * that the step creates in statements of their own. The step creates each table and index only if
   * it does not exist, so it can't add what a table lacks.
   */
  private boolean isLeftByAStoppedRun(String step, SchemaSnapshot schema, SchemaSnapshot after) {
    if (!after.entries().containsAll(schema.entries())) {
      return false;
    }
    Set<String> tables =
        schema.entries().stream()
            .filter(e -> e.startsWith("table "))
            .map(e -> e.substring("table ".length()))
            .collect(Collectors.toSet());
    Set<String> ownIndexes = indexesCreatedSeparately(step);
    return after.entries().stream()
        .filter(e -> !schema.entries().contains(e) && tables.contains(tableOf(e)))
        .allMatch(e -> e.startsWith("index ") && ownIndexes.contains(e.split(" ")[1]));
  }

  /** The table of a schema entry: the first part of its second word. */
  private static String tableOf(String entry) {
    String object = entry.split(" ")[1];
    int dot = object.indexOf('.');
    return dot < 0 ? object : object.substring(0, dot);
  }

  /**
   * The indexes, as {@code table.index}, that {@code step}'s script creates in a {@code CREATE
   * INDEX} statement of their own.
   */
  private Set<String> indexesCreatedSeparately(String step) {
    String script =
        resolvedSteps(flyway().load().info()).stream()
            .filter(m -> m.getVersion().getVersion().equals(step))
            .map(MigrationInfo::getScript)
            .findFirst()
            .orElseThrow();
    String path = "db/migration/" + kind.folder() + "/" + script;
    try (InputStream in = SchemaMigrator.class.getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException(
            "This build has no script " + path + " for its step " + step);
      }
      Matcher m = CREATE_INDEX.matcher(new String(in.readAllBytes(), StandardCharsets.UTF_8));
      Set<String> indexes = new HashSet<>();
      while (m.find()) {
        indexes.add(
            m.group(2).toLowerCase(Locale.ROOT) + "." + m.group(1).toLowerCase(Locale.ROOT));
      }
      return indexes;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Whether the database has no applied step in its schema history (no history table, an empty one,
   * or one holding only Flyway's baseline row, as a run leaves that stopped before step 1's record)
   * and the schema of this build's last step, exactly: what Hibernate's update builds from the
   * entities on a new database, with a {@code uc_*} table other than the history table. Every step
   * is then recorded without running.
   */
  private boolean hasLastStepsSchemaWithoutHistory() {
    String recorded = recordedVersion();
    boolean noAppliedStep = recorded == null || recorded.equals(EMPTY_BASELINE_VERSION);
    return noAppliedStep
        && tableNamesLowerCase().stream()
            .anyMatch(t -> t.startsWith("uc_") && !t.equals(HISTORY_TABLE))
        && SchemaSnapshot.of(dataSource).equals(expected(latestVersion()));
  }

  /** The step before {@code step} in this build, {@value #EMPTY_BASELINE_VERSION} for the first. */
  private String previousStep(String step) {
    String previous = EMPTY_BASELINE_VERSION;
    for (String s : steps()) {
      if (s.equals(step)) {
        return previous;
      }
      previous = s;
    }
    throw new IllegalArgumentException("Not a step of this build: " + step);
  }

  /** This build's steps after the recorded version. */
  private List<String> pendingSteps() {
    MigrationInfoService info = flyway().load().info();
    String recorded = tableNamesLowerCase().contains(HISTORY_TABLE) ? recordedVersion(info) : null;
    return stepVersions(info).stream()
        .filter(
            step ->
                recorded == null
                    || MigrationVersion.fromVersion(step)
                            .compareTo(MigrationVersion.fromVersion(recorded))
                        > 0)
        .toList();
  }

  /**
   * Deletes the schema history's record of {@code step} as a failed step, if it has one, so that
   * the step is tried again. Plain SQL: Flyway's repair would also rewrite the other records'
   * checksums.
   */
  private void deleteFailedRecord(String step) {
    if (!tableNamesLowerCase().contains(HISTORY_TABLE)) {
      return;
    }
    try (Connection connection = dataSource.getConnection()) {
      // The caller's pool may not commit by itself, and a rollback would keep the record.
      connection.setAutoCommit(true);
      String q = connection.getMetaData().getIdentifierQuoteString();
      try (PreparedStatement delete =
          connection.prepareStatement(
              "DELETE FROM %1$s%2$s%1$s WHERE %1$ssuccess%1$s = false AND %1$sversion%1$s = ?"
                  .formatted(q, HISTORY_TABLE))) {
        delete.setString(1, step);
        if (delete.executeUpdate() > 0) {
          LOGGER.info("Deleted the record of failed schema step {}; it is tried again", step);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not delete the record of a failed schema step: " + e.getMessage(), e);
    }
  }

  /**
   * Throws unless Flyway's validation of the schema history passes: no failed step, no edited step
   * (checksum), no step missing below the recorded version. With {@code upgrading}, steps after the
   * recorded version may be pending and may have failed: the upgrade runs them. A failed step that
   * is not one of this build's pending steps, such as a newer release's, is still refused.
   */
  private void requireValidHistory(boolean upgrading) {
    if (!tableNamesLowerCase().contains(HISTORY_TABLE)) {
      return;
    }
    FluentConfiguration configuration = flyway();
    Set<String> retried = upgrading ? Set.copyOf(pendingSteps()) : Set.of();
    if (upgrading) {
      configuration.ignoreMigrationPatterns("*:pending");
    }
    String errors =
        configuration.load().validateWithResult().invalidMigrations.stream()
            .filter(
                m ->
                    !(m.errorDetails.errorCode == CoreErrorCode.FAILED_VERSIONED_MIGRATION
                        && retried.contains(m.version)))
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

  /**
   * The schema that this build's steps up to {@code version} leave, added up from their effect
   * files {@code schema-effects/<database>/V<step>.txt}; {@link SchemaSnapshot#NONE} for version 0.
   *
   * @throws IllegalStateException when an effect file is missing or does not fit
   */
  SchemaSnapshot expected(String version) {
    MigrationVersion upTo = MigrationVersion.fromVersion(version);
    SchemaSnapshot schema = SchemaSnapshot.NONE;
    for (String step : steps()) {
      if (MigrationVersion.fromVersion(step).compareTo(upTo) > 0) {
        break;
      }
      String path = "schema-effects/" + kind.folder() + "/V" + step + ".txt";
      try (InputStream in = SchemaMigrator.class.getClassLoader().getResourceAsStream(path)) {
        if (in == null) {
          throw new IllegalStateException(
              "This build has no effect file "
                  + path
                  + " for schema step "
                  + step
                  + ": it is incomplete. Use a release build of Unity Catalog.");
        }
        schema =
            schema.with(new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException("The effect file " + path + " " + e.getMessage(), e);
      }
    }
    return schema;
  }

  /**
   * For {@code server.schema-management=hibernate}, before Hibernate updates the schema: throws
   * when the schema history records a step newer than this build's last, which Hibernate's update
   * would change back. Changes nothing. When the configuration doesn't let the schema history be
   * read (for example a PostgreSQL connection without a current schema), logs that and checks
   * nothing, since Hibernate mode doesn't need the schema steps' settings.
   */
  public static void refuseNewerSchema(DataSource dataSource) {
    SchemaMigrator migrator;
    try {
      migrator = new SchemaMigrator(dataSource);
    } catch (IllegalStateException e) {
      LOGGER.warn("Could not check for a schema newer than this release: {}", e.getMessage());
      return;
    }
    if (migrator.state() == SchemaState.AHEAD) {
      migrator.refuseIfUnsupported(SchemaState.AHEAD);
    }
  }

  /**
   * Throws for a database that {@link #upgradeSchema()} must not change, or a build with a
   * repeatable step.
   */
  private void refuseIfUnsupported(SchemaState state) {
    String repeatable =
        Arrays.stream(flyway().load().info().all())
            // Only this build's steps: the history may record steps of another build.
            .filter(
                m ->
                    m.getState().isResolved()
                        && m.getVersion() == null
                        && !m.getType().isSynthetic())
            .map(MigrationInfo::getScript)
            .findFirst()
            .orElse(null);
    if (repeatable != null) {
      throw new IllegalStateException(
          "This build has a repeatable schema step, which is not supported: " + repeatable);
    }
    String refusal =
        switch (state) {
          case AHEAD ->
              "The schema is newer than this release (version "
                  + recordedVersion()
                  + ", this release ends at "
                  + latestVersion()
                  + "). Run the newer release.";
          default -> null;
        };
    if (refusal != null) {
      throw new IllegalStateException(refusal);
    }
  }

  /**
   * Throws unless the database is at this build's last step: recorded as applied, Flyway's
   * validation passes, and the schema is the one the steps' effect files add up to.
   */
  private void requireCurrent() {
    SchemaState state = state();
    if (state != SchemaState.CURRENT) {
      throw new IllegalStateException(
          "The database schema is " + state + "; this release needs version " + latestVersion());
    }
    requireValidHistory(false);
    requireLastStepsSchema("");
  }

  /**
   * Throws unless the schema is the one that the steps' effect files add up to for this build's
   * last step, naming the differences and then {@code then}.
   */
  private void requireLastStepsSchema(String then) {
    SchemaSnapshot schema = SchemaSnapshot.of(dataSource);
    SchemaSnapshot expected = expected(latestVersion());
    if (!schema.equals(expected)) {
      throw new IllegalStateException(
          "The database schema differs from the schema of version "
              + latestVersion()
              + ":\n"
              + differences(schema, expected)
              + "\nRepair these differences (see docs/server/upgrades.md) or restore the backup"
              + then
              + ".");
    }
  }

  /**
   * {@code schema}'s differences from {@code expected}. Each missing index that another table's
   * index of the same name took (see {@link #indexClashHint}) gets that clash appended.
   */
  private String differences(SchemaSnapshot schema, SchemaSnapshot expected) {
    String differences = schema.differencesFrom(expected);
    if (kind == DatabaseKind.MYSQL) {
      return differences; // its index names are per table: no clash
    }
    try (Connection connection = dataSource.getConnection()) {
      StringBuilder out = new StringBuilder();
      for (String line : differences.split("\n")) {
        out.append(line);
        Matcher m = MISSING_INDEX.matcher(line);
        String hint = m.find() ? indexClashHint(connection, m.group(2), m.group(1)) : null;
        out.append(hint == null ? "" : " (" + hint + ")").append('\n');
      }
      return out.toString().stripTrailing();
    } catch (SQLException e) {
      throw new IllegalStateException("Could not check the schema's indexes: " + e.getMessage(), e);
    }
  }

  private static final Pattern MISSING_INDEX = Pattern.compile("^missing index (uc_\\w+)\\.(\\w+)");

  /**
   * For the index {@code index} missing on {@code table}: when a table that is not Unity Catalog's
   * ({@code uc_*}) has an index of that name, the clash: "index idx on other_app, so ... could not
   * create it on uc_x ...". {@code null} when the index is missing for another reason.
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
      if (!other.toLowerCase(Locale.ROOT).startsWith("uc_")
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

    /**
     * H2 has no lock call, and only one process opens its database, so runs in this process take
     * turns by database URL: servers on one in-memory database start in one process.
     */
    private static final Map<String, ReentrantLock> H2_LOCKS = new ConcurrentHashMap<>();

    private final Connection connection; // null on H2
    private final String unlockSql;
    private final ReentrantLock h2Lock; // null except on H2

    private RunLock(Connection connection, String unlockSql, ReentrantLock h2Lock) {
      this.connection = connection;
      this.unlockSql = unlockSql;
      this.h2Lock = h2Lock;
    }

    /**
     * Blocks until this run holds the lock, logging once when another run holds it. Throws when the
     * database fails the lock call or does not grant the lock, so the run never starts unlocked.
     */
    static RunLock acquire(DataSource dataSource, DatabaseKind kind) {
      if (kind == DatabaseKind.H2) {
        String url;
        try (Connection connection = dataSource.getConnection()) {
          url = connection.getMetaData().getURL();
        } catch (SQLException e) {
          throw new IllegalStateException(
              "Could not connect to the database: " + e.getMessage(), e);
        }
        ReentrantLock lock = H2_LOCKS.computeIfAbsent(url, u -> new ReentrantLock());
        lock.lock();
        return new RunLock(null, null, lock);
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
        return new RunLock(connection, calls.unlock(), null);
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
      if (h2Lock != null) {
        h2Lock.unlock();
        return;
      }
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
