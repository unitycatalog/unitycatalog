package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.migrate.SchemaMigrator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.h2.api.ErrorCode;

/**
 * Hands out a fresh {@link TestDatabase} on the database selected by {@value
 * SupportedDatabase#ENV}.
 */
public final class TestDatabases {

  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  private TestDatabases() {}

  public static TestDatabase newDatabase() {
    SupportedDatabase selected = SupportedDatabase.selected();
    String name = "uc_" + NEXT_ID.incrementAndGet();
    return switch (selected) {
      case H2 -> h2(name);
      case POSTGRES_18 -> PostgresDatabases.get().newDatabase(name);
      case MYSQL_8 -> MySqlDatabases.get().newDatabase(name);
    };
  }

  /** A database without tables, for tests that build the schema themselves. */
  public static TestDatabase newEmptyDatabase() {
    String name = "uc_" + NEXT_ID.incrementAndGet();
    return switch (SupportedDatabase.selected()) {
      case H2 -> h2File();
      case POSTGRES_18 -> PostgresDatabases.get().newEmptyDatabase(name);
      case MYSQL_8 -> MySqlDatabases.get().newEmptyDatabase(name);
    };
  }

  private static TestDatabase h2File() {
    Path directory;
    try {
      directory = Files.createTempDirectory("uc-h2-");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    String url = "jdbc:h2:file:" + directory.resolve("db");
    return new TestDatabase("org.h2.Driver", url, "sa", "", () -> delete(directory));
  }

  private static void delete(Path directory) {
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static TestDatabase h2(String name) {
    String url = "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1";
    // DB_CLOSE_DELAY=-1 keeps an in-memory database until SHUTDOWN, so release it explicitly.
    TestDatabase database = new TestDatabase("org.h2.Driver", url, "sa", "", () -> shutdown(url));
    new SchemaMigrator(database.dataSource()).migrate();
    return database;
  }

  private static void shutdown(String url) {
    // H2 creates an in-memory database on first connection; IFEXISTS keeps this from creating one
    // just to shut it down. A test that pointed elsewhere never created it: nothing to release.
    try (Connection connection = DriverManager.getConnection(url + ";IFEXISTS=TRUE", "sa", "")) {
      connection.createStatement().execute("SHUTDOWN");
    } catch (SQLException e) {
      if (e.getErrorCode() != ErrorCode.DATABASE_NOT_FOUND_WITH_IF_EXISTS_1) {
        throw new IllegalStateException("Could not shut down " + url, e);
      }
    }
  }
}
