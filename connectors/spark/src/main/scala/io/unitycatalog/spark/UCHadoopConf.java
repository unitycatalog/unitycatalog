package io.unitycatalog.spark;

public class UCHadoopConf {
  public UCHadoopConf() {
  }

  // Initialized configuration keys for the first credentials.
  public static final String UC_INIT_ACCESS_KEY = "fs.unitycatalog.init.access.key";
  public static final String UC_INIT_SECRET_KEY = "fs.unitycatalog.init.secret.key";
  public static final String UC_INIT_SESSION_TOKEN = "fs.unitycatalog.init.session.token";
  // Expired time in milliseconds.
  public static final String UC_INIT_EXPIRED_TIME = "fs.unitycatalog.init.expired.time";

  public static final String UC_URI = "fs.unitycatalog.uri";
  public static final String UC_TOKEN = "fs.unitycatalog.token";

  // Configuration keys for table based temporary credential requests
  public static final String UC_TABLE_ID = "fs.unitycatalog.table.id";
  public static final String UC_TABLE_OPERATION = "fs.unitycatalog.table.operation";

  // Configuration keys for path based temporary credential requests.
  public static final String UC_PATH = "fs.unitycatalog.path";
  public static final String UC_PATH_OPERATION = "fs.unitycatalog.path.operation";

  // Configuration key indicating the credential request type, table or path.
  public static final String UC_CREDENTIALS_TYPE = "fs.unitycatalog.credentials.type";
  public static final String UC_CREDENTIALS_TYPE_TABLE_VALUE = "table";
  public static final String UC_CREDENTIALS_TYPE_PATH_VALUE = "path";
}
