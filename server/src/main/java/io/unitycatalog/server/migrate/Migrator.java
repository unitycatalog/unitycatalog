package io.unitycatalog.server.migrate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;

/**
 * Sets up, adopts and upgrades Unity Catalog's schema with the steps of {@code
 * db/migration/<database>}. Each {@link #migrate()} runs under a {@link RunLock}: it classifies the
 * database, then sets up an empty one, adopts a 0.7 one (recording version 1 as the baseline) and
 * applies the pending steps.
 */
public final class Migrator implements AutoCloseable {

  private static final String HISTORY_TABLE = "flyway_schema_history";

  /** The version of the 0.7 schema, which Flyway records when it adopts a 0.7 database. */
  private static final String BASELINE_0_7 = "1";

  private static final String HIKARI_PREFIX = "hibernate.hikari.";

  private final DataSource dataSource;
  private final DatabaseKind kind;
  private final String[] locations;
  private final HikariDataSource ownedPool;

  /** Does not own {@code dataSource}. */
  public Migrator(DataSource dataSource, DatabaseKind kind) {
    this(dataSource, kind, List.of());
  }

  /** Also applies the steps in {@code extraLocations} (tests' own steps). */
  public Migrator(DataSource dataSource, DatabaseKind kind, List<String> extraLocations) {
    this(dataSource, kind, extraLocations, null);
  }

  private Migrator(
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
  public static Migrator fromHibernateProperties(Properties hibernateProperties) {
    Properties hikariProperties = new Properties();
    hibernateProperties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(HIKARI_PREFIX))
        .forEach(
            name ->
                hikariProperties.setProperty(
                    name.substring(HIKARI_PREFIX.length()), hibernateProperties.getProperty(name)));
    HikariConfig config = new HikariConfig(hikariProperties);
    String url = hibernateProperties.getProperty("hibernate.connection.url");
    config.setJdbcUrl(url);
    config.setUsername(
        hibernateProperties.getProperty(
            "hibernate.connection.username",
            hibernateProperties.getProperty("hibernate.connection.user")));
    config.setPassword(hibernateProperties.getProperty("hibernate.connection.password"));
    String driver = hibernateProperties.getProperty("hibernate.connection.driver_class");
    if (driver != null) {
      config.setDriverClassName(driver);
    }
    // The run lock's connection plus Flyway's two.
    config.setMaximumPoolSize(3);
    config.setMinimumIdle(0);
    config.setPoolName("uc-migrate");
    HikariDataSource pool = new HikariDataSource(config);
    return new Migrator(pool, DatabaseKind.fromJdbcUrl(url), List.of(), pool);
  }

  private Flyway flyway(String baselineVersion) { // null: no baselining
    FluentConfiguration configuration =
        Flyway.configure()
            .dataSource(dataSource)
            .locations(locations)
            .table(HISTORY_TABLE)
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

  private static String latestVersion(MigrationInfoService info) {
    return Arrays.stream(info.all())
        .filter(m -> m.getState().isResolved() && m.getVersion() != null)
        .map(MigrationInfo::getVersion)
        .max(MigrationVersion::compareTo)
        .map(MigrationVersion::getVersion)
        .orElseThrow();
  }

  public List<MigrationInfo> applied() {
    return List.of(flyway(null).info().applied());
  }

  /** The steps {@link #migrate()} would apply. */
  public List<MigrationInfo> pending() {
    List<MigrationInfo> pending = List.of(flyway(null).info().pending());
    if (state() != SchemaState.UNVERSIONED_UC) {
      return pending;
    }
    // Adopting a 0.7 database records the 0.7 schema as the baseline; only later steps run.
    return pending.stream().filter(m -> m.getVersion().isNewerThan(BASELINE_0_7)).toList();
  }

  /** Under the run lock: classifies the database, then sets it up, adopts it and upgrades it. */
  public MigrationOutcome migrate() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      return migrateLocked();
    }
  }

  /** Under the run lock: migrates an empty database (a first start); returns the state after. */
  public SchemaState prepareForStart() {
    try (RunLock lock = RunLock.acquire(dataSource, kind)) {
      SchemaState state = state();
      if (state == SchemaState.EMPTY) {
        migrateLocked();
        state = state();
      }
      return state;
    }
  }

  private MigrationOutcome migrateLocked() {
    SchemaState state = state(); // read under the lock
    switch (state) {
      case NOT_UC ->
          throw new IllegalStateException(
              "The database has tables but none of Unity Catalog's: not a Unity Catalog database."
                  + " Point the server at an empty database or a Unity Catalog one.");
      case AHEAD ->
          throw new IllegalStateException(
              "The schema is newer than this release (version "
                  + currentVersion()
                  + ", this release ends at "
                  + latestVersion()
                  + "). Run the newer release.");
      case FAILED ->
          throw new IllegalStateException(
              "A migration step failed in an earlier run. See docs/server/upgrades.md, then rerun"
                  + " bin/uc-migrate migrate.");
      default -> {}
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
}
