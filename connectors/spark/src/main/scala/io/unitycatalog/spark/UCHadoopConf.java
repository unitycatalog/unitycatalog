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
  public static final String UC_URI_KEY = "unitycatalog.uri";
  public static final String UC_TOKEN_KEY = "unitycatalog.token";

  // Keys for table based temporary credential requests
  public static final String UC_TABLE_ID_KEY = "unitycatalog.table.id";
  public static final String UC_TABLE_OPERATION_KEY = "unitycatalog.table.operation";

  // Keys for path based temporary credential requests.
  public static final String UC_PATH_KEY = "unitycatalog.path";
  public static final String UC_PATH_OPERATION_KEY = "unitycatalog.path.operation";

  // Key indicating the credential request type, table or path.
  public static final String UC_CREDENTIALS_TYPE_KEY = "unitycatalog.credentials.type";
  public static final String UC_CREDENTIALS_TYPE_TABLE_VALUE = "table";
  public static final String UC_CREDENTIALS_TYPE_PATH_VALUE = "path";

  // Key to enable the credential cache.
  public static final String UC_CREDENTIAL_CACHE_ENABLED_KEY =
      "unitycatalog.credential.cache.enabled";
  public static final boolean UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE = true;
}
