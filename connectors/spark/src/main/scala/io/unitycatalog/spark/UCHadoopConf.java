package io.unitycatalog.spark;

public class UCHadoopConf {
  private UCHadoopConf() {
  }

  // Keys for the initialized aws s3 credentials.
  public static final String S3A_INIT_ACCESS_KEY = "fs.s3a.init.access.key";
  public static final String S3A_INIT_SECRET_KEY = "fs.s3a.init.secret.key";
  public static final String S3A_INIT_SESSION_TOKEN = "fs.s3a.init.session.token";
  // Expired time in milliseconds.
  public static final String S3A_INIT_CRED_EXPIRED_TIME = "fs.s3a.init.credential.expired.time";

  // Keys for the UnityCatalog client.
  public static final String UC_URI = "fs.unitycatalog.uri";
  public static final String UC_TOKEN = "fs.unitycatalog.token";

  // Keys for table based temporary credential requests
  public static final String UC_TABLE_ID = "fs.unitycatalog.table.id";
  public static final String UC_TABLE_OPERATION = "fs.unitycatalog.table.operation";

  // Keys for path based temporary credential requests.
  public static final String UC_PATH = "fs.unitycatalog.path";
  public static final String UC_PATH_OPERATION = "fs.unitycatalog.path.operation";

  // Key indicating the credential request type, table or path.
  public static final String UC_CREDENTIALS_TYPE = "fs.unitycatalog.credentials.type";
  public static final String UC_CREDENTIALS_TYPE_TABLE_VALUE = "table";
  public static final String UC_CREDENTIALS_TYPE_PATH_VALUE = "path";
}
