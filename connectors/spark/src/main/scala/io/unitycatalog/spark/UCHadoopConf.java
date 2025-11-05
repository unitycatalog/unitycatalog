package io.unitycatalog.spark;

public class UCHadoopConf {
  private UCHadoopConf() {
  }

  // Key for the AWS S3 credential provider, same as org.apache.hadoop.fs.s3a.Constants
  // #AWS_CREDENTIALS_PROVIDER, but defined here to avoid an extra hadoop-aws dependency.
  public static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";

  // Keys for the initialized aws s3 credentials.
  public static final String S3A_INIT_ACCESS_KEY = "fs.s3a.init.access.key";
  public static final String S3A_INIT_SECRET_KEY = "fs.s3a.init.secret.key";
  public static final String S3A_INIT_SESSION_TOKEN = "fs.s3a.init.session.token";
  // Expired time in milliseconds.
  public static final String S3A_INIT_CRED_EXPIRED_TIME = "fs.s3a.init.credential.expired.time";

  // Keys for the initialized Azure Blob Storage token.
  public static final String AZURE_INIT_SAS_TOKEN = "fs.azure.init.sas.token";
  public static final String AZURE_INIT_SAS_TOKEN_EXPIRED_TIME =
      "fs.azure.init.sas.token.expired.time";

  // Keys for the initialized Google Cloud Storage OAuth token.
  public static final String GCS_INIT_OAUTH_TOKEN = "fs.gs.init.oauth.token";
  public static final String GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME =
      "fs.gs.init.oauth.token.expiration.time";

  // Keys for the UnityCatalog client.
  // Note: Delta automatically filters out configuration keys without the "fs." prefix To ensure
  // custom configuration keys (e.g., AWS, ABFS, or GCS credentials) propagate correctly into Hadoop
  // FileSystems such as S3AFileSystem or AbfsFileSystem, the keys must include the "fs." prefix.
  // See also: https://github.com/unitycatalog/unitycatalog/issues/1112.
  public static final String UC_URI_KEY = "fs.unitycatalog.uri";
  public static final String UC_TOKEN_KEY = "fs.unitycatalog.token";

  // Key representing the remaining time before expiration, used to trigger credentials renewal in
  // advance.
  public static final String UC_RENEWAL_LEAD_TIME_KEY = "fs.unitycatalog.renewal.leadTimeMillis";
  public static final long UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE = 30_000L;

  // Key for specifying the manual clock, for testing purpose.
  public static final String UC_TEST_CLOCK_NAME = "fs.unitycatalog.test.clock.name";

  // Key representing a unique credential ID. It identifies a job-level credential for a specific
  // table, meaning that the same job–table combination shares the same credential. Cached
  // credentials are indexed by this key and are not reused across different jobs.
  public static final String UC_CREDENTIALS_UID_KEY = "fs.unitycatalog.credentials.uid";

  // Keys for table based temporary credential requests
  public static final String UC_TABLE_ID_KEY = "fs.unitycatalog.table.id";
  public static final String UC_TABLE_OPERATION_KEY = "fs.unitycatalog.table.operation";

  // Keys for path based temporary credential requests.
  public static final String UC_PATH_KEY = "fs.unitycatalog.path";
  public static final String UC_PATH_OPERATION_KEY = "fs.unitycatalog.path.operation";

  // Key indicating the credential request type, table or path.
  public static final String UC_CREDENTIALS_TYPE_KEY = "fs.unitycatalog.credentials.type";
  public static final String UC_CREDENTIALS_TYPE_TABLE_VALUE = "table";
  public static final String UC_CREDENTIALS_TYPE_PATH_VALUE = "path";

  // Key to enable the credential cache.
  public static final String UC_CREDENTIAL_CACHE_ENABLED_KEY =
      "fs.unitycatalog.credential.cache.enabled";
  public static final boolean UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE = true;
}
