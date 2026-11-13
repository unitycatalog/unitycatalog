package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.utils.ServerProperties.DB_PASSWORD_KEY;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.migrate.SchemaMigrator;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.FunctionParameterInfoDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Getter;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.cfg.JdbcSettings;
import org.hibernate.cfg.SchemaToolingSettings;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class configures the hibernate properties and adds annotated classes to the session factory.
 * This session factory is used to create sessions for database operations across the repository
 * classes. Casbin's JDBC adapter is given the same DataSource so both sit under one Hikari {@code
 * maximumPoolSize}. jdbc-adapter 2.7.0 still holds one pooled connection for its lifetime; Casbin
 * enables autocommit on that checkout only.
 */
@Getter
public class HibernateConfigurator implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateConfigurator.class);
  private static final String HIKARI_PREFIX = "hibernate.hikari.";

  private final SessionFactory sessionFactory;
  private final Properties hibernateProperties;
  private final HikariDataSource dataSource;
  private final boolean schemaManagedByHibernate;

  public HibernateConfigurator(ServerProperties serverProperties) {
    this(setupHibernateProperties(serverProperties), serverProperties.isSchemaManagedByHibernate());
  }

  /**
   * Builds a session factory from explicit hibernate properties. Lets tests customize the
   * properties (e.g. point at PostgreSQL via Testcontainers) before construction.
   */
  public HibernateConfigurator(Properties hibernateProperties) {
    this(hibernateProperties, false);
  }

  /**
   * The same, where {@code schemaManagedByHibernate} lets Hibernate update the schema instead of
   * the schema steps ({@code server.schema-management=hibernate}), whatever schema action the
   * properties set.
   */
  public HibernateConfigurator(Properties hibernateProperties, boolean schemaManagedByHibernate) {
    if (schemaManagedByHibernate) {
      hibernateProperties = withSchemaUpdatesOn(hibernateProperties);
    }
    Map<Object, Object> settings = settingsHibernateReads(hibernateProperties);
    if (!schemaManagedByHibernate) {
      // Only the schema steps use the connection's schema; in hibernate mode none runs.
      requireSchemaFromConnection(hibernateProperties);
      requireNoSchemaChanges(settings);
    }
    requireNamedMemoryDatabase(hibernateProperties);
    this.schemaManagedByHibernate = schemaManagedByHibernate;
    this.hibernateProperties = hibernateProperties;
    this.dataSource = createDataSource(hibernateProperties);
    try {
      if (schemaManagedByHibernate) {
        // Before Hibernate's update, which would change a newer release's schema back.
        // Read from the settings Hibernate reads, JVM system properties too.
        SchemaMigrator.refuseNewerSchema(dataSource, settings);
      }
      this.sessionFactory = createSessionFactory(hibernateProperties, dataSource);
    } catch (Throwable t) {
      dataSource.close();
      throw t;
    }
    warnIfMySqlIsCaseInsensitive(hibernateProperties);
  }

  /**
   * {@code properties} over the settings Hibernate adds itself: JVM system properties and a {@code
   * hibernate.properties} on the classpath.
   */
  private static Map<Object, Object> settingsHibernateReads(Properties properties) {
    Map<Object, Object> settings = new HashMap<>(Environment.getProperties());
    settings.putAll(properties);
    return settings;
  }

  /**
   * The schema migration and Hibernate must use the same schema, and the migration uses the
   * connection's. Refuses Hibernate's own default schema or catalog, which only Hibernate would
   * use.
   */
  public static void requireSchemaFromConnection(Properties hibernateProperties) {
    Map<Object, Object> settings = settingsHibernateReads(hibernateProperties);
    for (String name : new String[] {"hibernate.default_schema", "hibernate.default_catalog"}) {
      if (settings.get(name) != null) {
        throw new IllegalArgumentException(
            name
                + " is not supported: the schema migration uses the connection's schema. Remove it"
                + " and set the schema in the JDBC URL instead (PostgreSQL: currentSchema=...).");
      }
    }
  }

  /**
   * An H2 in-memory URL without a name ({@code jdbc:h2:mem:} or {@code jdbc:h2:.}) gives every
   * connection its own empty database, so the server would not see the schema that was set up.
   * Refuses it.
   */
  private static void requireNamedMemoryDatabase(Properties hibernateProperties) {
    for (String key : urlSettings(hibernateProperties)) {
      String url = hibernateProperties.getProperty(key);
      String h2 = h2Url(url);
      if (h2 != null && h2.matches("jdbc:h2:(mem:|\\.)(;.*)?")) {
        throw new IllegalArgumentException(
            "The in-memory H2 URL "
                + url
                + " has no database name, so every connection gets its own empty database. Name"
                + " it: jdbc:h2:mem:<name>;DB_CLOSE_DELAY=-1");
      }
    }
  }

  /**
   * The settings that can give the pool its JDBC URL: {@code hibernate.connection.url}, and the URL
   * property of a data source class ({@code hibernate.hikari.dataSource.URL} or {@code .url}).
   */
  public static List<String> urlSettings(Properties hibernateProperties) {
    return hibernateProperties.stringPropertyNames().stream()
        .filter(
            key ->
                key.equals("hibernate.connection.url")
                    || (key.startsWith(HIKARI_DATA_SOURCE)
                        && key.substring(HIKARI_DATA_SOURCE.length()).equalsIgnoreCase("url")))
        .sorted()
        .toList();
  }

  private static final String HIKARI_DATA_SOURCE = "hibernate.hikari.dataSource.";

  /**
   * The schema steps own the schema. Refuses every Hibernate schema action but none, validate too,
   * before the pool or the session factory exists.
   */
  private static void requireNoSchemaChanges(Map<Object, Object> settings) {
    settings.forEach(
        (name, value) -> {
          if (isSchemaAction(name) && !isNone(value)) {
            throw new IllegalStateException(
                name
                    + "="
                    + value
                    + " is not supported: the schema steps manage the schema. Set it to none.");
          }
        });
  }

  /**
   * A schema action setting, or its form for one contributor ({@code hibernate.hbm2ddl.auto.orm}).
   */
  private static boolean isSchemaAction(Object name) {
    return name instanceof String setting
        && Stream.of(
                SchemaToolingSettings.HBM2DDL_AUTO,
                SchemaToolingSettings.JAKARTA_HBM2DDL_DATABASE_ACTION,
                SchemaToolingSettings.HBM2DDL_DATABASE_ACTION)
            .anyMatch(action -> setting.equals(action) || setting.startsWith(action + "."));
  }

  /** Hibernate takes an {@link Action} or its name, and reads an empty value as none. */
  private static boolean isNone(Object value) {
    return value instanceof Action action
        ? action == Action.NONE
        : value.toString().isBlank() || "none".equalsIgnoreCase(value.toString().trim());
  }

  /**
   * Storage paths compare exactly, but MySQL's default collation ignores case. Warns when any text
   * column in the connected MySQL schema has a case-insensitive ({@code _ci}) collation. A failed
   * check never fails startup.
   */
  private void warnIfMySqlIsCaseInsensitive(Properties properties) {
    if (!properties.getProperty("hibernate.connection.url", "").startsWith("jdbc:mysql")) {
      return;
    }
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet collations =
            statement.executeQuery(
                "SELECT DISTINCT COLLATION_NAME FROM information_schema.COLUMNS "
                    + "WHERE TABLE_SCHEMA = DATABASE() AND COLLATION_NAME IS NOT NULL")) {
      Set<String> caseInsensitive = new TreeSet<>();
      while (collations.next()) {
        String collation = collations.getString(1);
        if (collation.endsWith("_ci")) {
          caseInsensitive.add(collation);
        }
      }
      if (!caseInsensitive.isEmpty()) {
        LOGGER.warn(
            "The MySQL database compares text case-insensitively (collation {}), so storage paths"
                + " that differ only in case are treated as the same. Create the"
                + " database with COLLATE utf8mb4_0900_bin; see the MySQL section of"
                + " docs/server/deployment.md.",
            caseInsensitive);
      }
    } catch (SQLException | RuntimeException e) {
      // Only a warning is lost, and an exception here would leak the pool: the constructor does not
      // close it after this point.
      LOGGER.warn("Could not check whether the MySQL database compares text case-insensitively", e);
    }
  }

  private static HikariDataSource createDataSource(Properties hibernateProperties) {
    return new HikariDataSource(hikariConfig(hibernateProperties));
  }

  /** The connection pool settings of {@code hibernateProperties}. */
  public static HikariConfig hikariConfig(Properties hibernateProperties) {
    Properties hikariProperties = new Properties();
    hibernateProperties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(HIKARI_PREFIX))
        .forEach(
            name ->
                hikariProperties.setProperty(
                    name.substring(HIKARI_PREFIX.length()), hibernateProperties.getProperty(name)));

    HikariConfig hikariConfig = new HikariConfig(hikariProperties);
    hikariConfig.setJdbcUrl(hibernateProperties.getProperty("hibernate.connection.url"));
    hikariConfig.setUsername(resolveConnectionUsername(hibernateProperties));
    hikariConfig.setPassword(hibernateProperties.getProperty(DB_PASSWORD_KEY));
    hikariConfig.setDriverClassName(
        hibernateProperties.getProperty("hibernate.connection.driver_class"));
    if (!hikariProperties.containsKey("maximumPoolSize")) {
      hikariConfig.setMaximumPoolSize(
          Integer.parseInt(
              hibernateProperties.getProperty("hibernate.connection.pool_size", "20")));
    }
    if (!hikariProperties.containsKey("minimumIdle")) {
      hikariConfig.setMinimumIdle(Math.min(2, hikariConfig.getMaximumPoolSize()));
    }
    if (!hikariProperties.containsKey("autoCommit")) {
      // Hibernate manages transactions and issues rollback() when JDBC work ends.
      hikariConfig.setAutoCommit(false);
    }
    if (hikariConfig.getPoolName() == null) {
      hikariConfig.setPoolName("unity-catalog");
    }
    return hikariConfig;
  }

  /**
   * Prefers {@code hibernate.connection.username} and falls back to {@code
   * hibernate.connection.user}, which the deployment examples still document.
   */
  static String resolveConnectionUsername(Properties properties) {
    String username = properties.getProperty("hibernate.connection.username");
    return username != null ? username : properties.getProperty("hibernate.connection.user");
  }

  private static SessionFactory createSessionFactory(
      Properties hibernateProperties, HikariDataSource dataSource) {
    try {
      Properties sessionFactoryProperties = new Properties();
      sessionFactoryProperties.putAll(hibernateProperties);
      sessionFactoryProperties.remove("hibernate.connection.driver_class");
      sessionFactoryProperties.remove("hibernate.connection.url");
      sessionFactoryProperties.remove("hibernate.connection.user");
      sessionFactoryProperties.remove("hibernate.connection.username");
      sessionFactoryProperties.remove(DB_PASSWORD_KEY);
      sessionFactoryProperties.remove("hibernate.connection.pool_size");
      sessionFactoryProperties
          .keySet()
          .removeIf(key -> key instanceof String name && name.startsWith(HIKARI_PREFIX));
      sessionFactoryProperties.put(JdbcSettings.JAKARTA_NON_JTA_DATASOURCE, dataSource);

      Configuration configuration = new Configuration().setProperties(sessionFactoryProperties);

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(StagingTableDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(ModelVersionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);
      configuration.addAnnotatedClass(UserDAO.class);
      configuration.addAnnotatedClass(MetastoreDAO.class);
      configuration.addAnnotatedClass(CredentialDAO.class);
      configuration.addAnnotatedClass(ExternalLocationDAO.class);
      configuration.addAnnotatedClass(DeltaCommitDAO.class);
      configuration.addAnnotatedClass(DependencyDAO.class);
      configuration.addAnnotatedClass(StorageCleanupTaskDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }

  @Override
  public void close() {
    try {
      sessionFactory.close();
    } finally {
      dataSource.close();
    }
  }

  public static Properties setupHibernateProperties(ServerProperties serverProperties) {
    return setupHibernateProperties(serverProperties, Paths.get("etc/conf/hibernate.properties"));
  }

  /** The same, from the Hibernate properties file at {@code hibernatePropertiesPath}. */
  public static Properties setupHibernateProperties(
      ServerProperties serverProperties, Path hibernatePropertiesPath) {
    Properties hibernateProperties = new Properties();
    if (!hibernatePropertiesPath.toFile().exists()) {
      LOGGER.warn("Hibernate properties file not found: {}", hibernatePropertiesPath);
      hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      hibernateProperties.setProperty(
          "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
    } else {
      try (InputStream input = Files.newInputStream(hibernatePropertiesPath)) {
        hibernateProperties.load(input);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    applyPasswordOverride(
        hibernateProperties, serverProperties.getEnv(DB_PASSWORD_KEY), hibernatePropertiesPath);

    if ("test".equals(serverProperties.get(Property.SERVER_ENV))) {
      hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      hibernateProperties.setProperty(
          "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
      LOGGER.debug("Hibernate configuration set for testing");
    }
    return serverProperties.isSchemaManagedByHibernate()
        ? hibernateProperties
        : withSchemaUpdatesOff(hibernateProperties);
  }

  /**
   * A copy of {@code properties} in which Hibernate updates the schema: the schema action is the
   * server's. Every schema action setting that Hibernate reads, from the file as well as from JVM
   * system properties and a classpath {@code hibernate.properties} (see {@link
   * #settingsHibernateReads}), is set to {@code update}, so that none, for example a {@code .orm}
   * form for one contributor, can win. Hibernate reads the JPA action before {@code
   * hibernate.hbm2ddl.auto}, so that one is set too.
   */
  static Properties withSchemaUpdatesOn(Properties properties) {
    Properties withUpdates = new Properties();
    properties.forEach(
        (name, value) -> {
          if (!isSchemaAction(name)) {
            withUpdates.put(name, value);
          }
        });
    settingsHibernateReads(properties)
        .forEach(
            (name, value) -> {
              if (isSchemaAction(name)) {
                if (!isNone(value) && !"update".equalsIgnoreCase(value.toString().trim())) {
                  LOGGER.warn(
                      "Ignoring {}={}: with server.schema-management=hibernate, Hibernate updates"
                          + " the schema.",
                      name,
                      value);
                }
                withUpdates.put(name, "update");
              }
            });
    withUpdates.setProperty(SchemaToolingSettings.JAKARTA_HBM2DDL_DATABASE_ACTION, "update");
    return withUpdates;
  }

  /**
   * Flyway owns the schema, so Hibernate never changes it, even if an old {@code
   * etc/conf/hibernate.properties} still says {@code update}.
   */
  static Properties withSchemaUpdatesOff(Properties properties) {
    String auto = properties.getProperty("hibernate.hbm2ddl.auto");
    if (auto != null && !isNone(auto)) {
      LOGGER.warn(
          "Ignoring hibernate.hbm2ddl.auto={}: the schema is managed by the schema migration"
              + " steps.",
          auto);
    }
    properties.setProperty("hibernate.hbm2ddl.auto", "none");
    return properties;
  }

  /**
   * {@code url} lower-cased with the names a wrapping driver puts before {@code h2:} left out
   * ({@code jdbc:p6spy:h2:...} gives {@code jdbc:h2:...}), or {@code null} when it is not an H2
   * URL.
   */
  public static String h2Url(String url) {
    if (url == null) {
      return null;
    }
    Matcher h2 = H2_URL.matcher(url.toLowerCase(Locale.ROOT));
    return h2.matches() ? "jdbc:h2:" + h2.group(1) : null;
  }

  private static final Pattern H2_URL =
      Pattern.compile("jdbc:(?:[a-z0-9_.-]+:)*?h2:(.*)", Pattern.DOTALL);

  /**
   * Takes the database password from the environment ({@code UC_HIBERNATE_CONNECTION_PASSWORD})
   * when it is set. A password kept in hibernate.properties still works but logs a deprecation
   * warning. A system property is not used for the password: Hibernate copies system properties
   * into its own settings, and a password there makes the connection pool fail.
   */
  static void applyPasswordOverride(
      Properties hibernateProperties, String password, Path propertiesPath) {
    if (password != null) {
      hibernateProperties.setProperty(DB_PASSWORD_KEY, password);
    } else if (!hibernateProperties.getProperty(DB_PASSWORD_KEY, "").isEmpty()) {
      LOGGER.warn(
          "{} is set in {}. Keeping the database password in this file is deprecated; set the {}"
              + " environment variable instead.",
          DB_PASSWORD_KEY,
          propertiesPath,
          ServerProperties.envName(DB_PASSWORD_KEY));
    }
  }
}
