package io.unitycatalog.server.migrate;

/** The databases Unity Catalog's schema steps exist for. */
public enum DatabaseKind {
  H2("h2"),
  POSTGRESQL("postgresql"),
  MYSQL("mysql");

  private final String folder;

  DatabaseKind(String folder) {
    this.folder = folder;
  }

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
