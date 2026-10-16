package io.unitycatalog.server.testdatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

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
      case POSTGRES_18 -> PostgresDatabases.get(selected).newDatabase(name);
    };
  }

  private static TestDatabase h2(String name) {
    String url = "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1";
    // DB_CLOSE_DELAY=-1 keeps an in-memory database until SHUTDOWN, so release it explicitly.
    return new TestDatabase("org.h2.Driver", url, "sa", "", "create-drop", () -> shutdown(url));
  }

  private static void shutdown(String url) {
    try (Connection connection = DriverManager.getConnection(url, "sa", "")) {
      connection.createStatement().execute("SHUTDOWN");
    } catch (SQLException e) {
      throw new IllegalStateException("Could not shut down " + url, e);
    }
  }
}
