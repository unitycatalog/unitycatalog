package io.unitycatalog.server.testdatabase;

final class PostgresDatabases {
  static PostgresDatabases get(SupportedDatabase database) {
    throw new UnsupportedOperationException(database + " is not wired up yet");
  }

  TestDatabase newDatabase(String name) {
    throw new UnsupportedOperationException();
  }
}
