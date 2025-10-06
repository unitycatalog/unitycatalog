package io.unitycatalog.spark;

public class Constants {
  public Constants() {
  }

  // Initialized configuration keys for the first credentials.
  public static final String UNITY_CATALOG_INIT_ACCESS_KEY = "fs.unitycatalog.init.access.key";
  public static final String UNITY_CATALOG_INIT_SECRET_KEY = "fs.unitycatalog.init.secret.key";
  public static final String UNITY_CATALOG_INIT_SESSION_TOKEN =
      "fs.unitycatalog.init.session.token";
  // Expired time in milliseconds.
  public static final String UNITY_CATALOG_INIT_EXPIRED_TIME = "fs.unitycatalog.init.expired.time";

  public static final String UNITY_CATALOG_URI = "fs.unitycatalog.uri";
  public static final String UNITY_CATALOG_TOKEN = "fs.unitycatalog.token";

  // Configuration keys for table based temporary credential requests
  public static final String UNITY_CATALOG_TABLE = "fs.unitycatalog.table";
  public static final String UNITY_CATALOG_TABLE_OPERATION = "fs.unitycatalog.table.operation";

  // Configuration keys for path based temporary credential requests.
  public static final String UNITY_CATALOG_PATH = "fs.unitycatalog.path";
  public static final String UNITY_CATALOG_PATH_OPERATION = "fs.unitycatalog.path.operation";

  public static final String UNITY_CATALOG_CREDENTIALS_TYPE = "fs.unitycatalog.credentials.type";
  public static final String UNITY_CATALOG_TABLE_CREDENTIALS_TYPE = "table";
  public static final String UNITY_CATALOG_PATH_CREDENTIALS_TYPE = "path";
}
