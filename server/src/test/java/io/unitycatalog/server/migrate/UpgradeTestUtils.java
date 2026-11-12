package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.Callback;
import org.flywaydb.core.api.callback.Context;
import org.flywaydb.core.api.callback.Event;

/** Databases and statements the schema migration tests share. */
final class UpgradeTestUtils {

  private UpgradeTestUtils() {}

  /** Builds a database of the baseline release: its schema without Flyway's history. */
  static void createBaselineSchema(TestDatabase db) {
    String folder = DatabaseKind.of(db.dataSource()).folder();
    executeStatements(db, resource("db/migration/" + folder + "/V1__baseline_0_7.sql"));
  }

  /** The text of the resource {@code path}. */
  static String resource(String path) {
    try (InputStream in = UpgradeTestUtils.class.getClassLoader().getResourceAsStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Runs the statements of {@code sql}, one by one, leaving out lines that start with --. */
  static void executeStatements(TestDatabase db, String sql) {
    String withoutComments = sql.replaceAll("(?m)^\\s*--.*$", "");
    for (String statement : withoutComments.split(";")) {
      if (!statement.isBlank()) {
        execute(db, statement);
      }
    }
  }

  /** The schema the steps build on a new empty database. */
  static SchemaSnapshot freshInstallSnapshot() {
    try (TestDatabase fresh = TestDatabases.newEmptyDatabase()) {
      new SchemaMigrator(fresh.dataSource()).upgradeSchema();
      return SchemaSnapshot.of(fresh.dataSource());
    }
  }

  /** Runs this build's steps up to {@code version} on {@code db}, as a new install does. */
  static void migrateTo(TestDatabase db, String version) {
    Flyway.configure()
        .dataSource(db.dataSource())
        .locations("classpath:db/migration/" + DatabaseKind.of(db.dataSource()).folder())
        .table("uc_schema_history")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .target(version)
        .load()
        .migrate();
  }

  /**
   * Runs this build's steps on {@code db}, a new database, and returns the SQL statements Flyway
   * sent to the database while it ran each step's own SQL, by version: what the database ran, as
   * Flyway's parser split the step.
   */
  static Map<String, List<String>> sentStatements(TestDatabase db) {
    Map<String, List<String>> sent = new LinkedHashMap<>();
    AtomicReference<List<String>> step = new AtomicReference<>();
    Callback recorder =
        new Callback() {
          @Override
          public boolean supports(Event event, Context context) {
            return event == Event.BEFORE_EACH_MIGRATE
                || event == Event.AFTER_EACH_MIGRATE
                || event == Event.AFTER_EACH_MIGRATE_ERROR;
          }

          @Override
          public boolean canHandleInTransaction(Event event, Context context) {
            return true;
          }

          @Override
          public void handle(Event event, Context context) {
            if (event == Event.BEFORE_EACH_MIGRATE) {
              step.set(new ArrayList<>());
              sent.put(context.getMigrationInfo().getVersion().getVersion(), step.get());
            } else {
              step.set(null);
            }
          }

          @Override
          public String getCallbackName() {
            return "sent statements";
          }
        };
    DataSource watched =
        watched(
            db.dataSource(),
            (sql, prepared) -> {
              if (step.get() != null && !prepared) {
                step.get().add(sql);
              }
              return sql;
            });
    // The server's own configuration, so the recorder sees the steps the server would run.
    new SchemaMigrator(watched).flyway().callbacks(recorder).load().migrate();
    return sent;
  }

  /** What {@link #watched} calls with each SQL statement before it is sent. */
  interface SqlWatcher {
    /**
     * Returns the SQL to send instead of {@code sql}, or throws to make the statement fail. {@code
     * prepared}: the statement is prepared (Flyway's own queries and history records).
     */
    String beforeSending(String sql, boolean prepared) throws SQLException;
  }

  /**
   * {@code real}, but {@code watcher} sees each SQL statement before a connection sends or prepares
   * it, and can change it or make it fail.
   */
  static DataSource watched(DataSource real, SqlWatcher watcher) {
    return proxy(
        DataSource.class,
        (proxy, method, args) -> {
          Object result = method.invoke(real, args);
          if (!method.getName().equals("getConnection")) {
            return result;
          }
          Connection connection = (Connection) result;
          return proxy(
              Connection.class,
              (connectionProxy, connectionMethod, connectionArgs) -> {
                if (connectionMethod.getName().startsWith("prepare")) {
                  connectionArgs[0] = watcher.beforeSending((String) connectionArgs[0], true);
                }
                Object created = connectionMethod.invoke(connection, connectionArgs);
                if (!(created instanceof Statement statement)
                    || created instanceof PreparedStatement) {
                  return created;
                }
                return proxy(
                    Statement.class,
                    (statementProxy, statementMethod, statementArgs) -> {
                      if (statementArgs != null
                          && statementArgs.length > 0
                          && statementArgs[0] instanceof String sql) {
                        statementArgs = statementArgs.clone();
                        statementArgs[0] = watcher.beforeSending(sql, false);
                      }
                      return statementMethod.invoke(statement, statementArgs);
                    });
              });
        });
  }

  @SuppressWarnings("unchecked")
  static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              try {
                return handler.invoke(proxy, method, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
