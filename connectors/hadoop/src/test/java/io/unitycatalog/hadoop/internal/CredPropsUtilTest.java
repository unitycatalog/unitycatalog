package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link io.unitycatalog.hadoop.internal.CredPropsUtil} saves the original {@code
 * fs.<scheme>.impl} values under {@code fs.<scheme>.impl.original} before overriding them with
 * CredScopedFileSystem, so that the real delegate can be restored in {@code
 * CredScopedFileSystem#newFileSystem}.
 */
class CredPropsUtilTest {

  private static final String CUSTOM_S3_IMPL = "com.example.CustomS3FileSystem";
  private static final String CUSTOM_GS_IMPL = "com.example.CustomGcsFileSystem";
  private static final String CUSTOM_ABFS_IMPL = "com.example.CustomAbfsFileSystem";
  private static final String CUSTOM_ABFSS_IMPL = "com.example.CustomAbfssFileSystem";

  @Test
  void s3OriginalImplPreservedFromExistingProps() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void s3DefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props.get("fs.s3.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
    assertThat(props.get("fs.s3a.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
  }

  @Test
  void gsOriginalImplPreservedFromExistingProps() {
    Configuration conf = new Configuration(false);
    conf.set("fs.gs.impl", CUSTOM_GS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "gs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            gcsCreds());

    assertThat(props.get("fs.gs.impl.original")).isEqualTo(CUSTOM_GS_IMPL);
  }

  @Test
  void abfsOriginalImplPreservedFromExistingProps() {
    Configuration conf = new Configuration(false);
    conf.set("fs.abfs.impl", CUSTOM_ABFS_IMPL);
    conf.set("fs.abfss.impl", CUSTOM_ABFSS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "abfs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            abfsCreds());

    assertThat(props.get("fs.abfs.impl.original")).isEqualTo(CUSTOM_ABFS_IMPL);
    assertThat(props.get("fs.abfss.impl.original")).isEqualTo(CUSTOM_ABFSS_IMPL);
  }

  @Test
  void gsDefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "gs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            gcsCreds());

    assertThat(props.get("fs.gs.impl.original"))
        .isEqualTo("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
  }

  @Test
  void abfsDefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "abfs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            abfsCreds());

    assertThat(props.get("fs.abfs.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    assertThat(props.get("fs.abfss.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
  }

  @Test
  void originalImplNotSetWhenCredScopedFsDisabled() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props).doesNotContainKey("fs.s3.impl.original");
    assertThat(props).doesNotContainKey("fs.s3a.impl.original");
  }

  // For unitycatalog delta table API.

  @Test
  void s3DeltaTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            CredentialOperation.READ_WRITE,
            s3Creds());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/tbl")
        .containsEntry(
            UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ_WRITE.getValue())
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_TABLE_ID_KEY);
  }

  @Test
  void gcsDeltaTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "gs://bucket/tbl",
            CredentialOperation.READ,
            gcsCreds());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "gs://bucket/tbl")
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue())
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token")
        .containsEntry(
            UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
            String.valueOf(Long.MAX_VALUE));
  }

  @Test
  void abfsDeltaTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfss",
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "abfss://container@account.dfs.core.windows.net/tbl",
            CredentialOperation.READ_WRITE,
            abfsCreds());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl")
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_LOCATION_KEY,
            "abfss://container@account.dfs.core.windows.net/tbl")
        .containsEntry(
            UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ_WRITE.getValue())
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas");
  }

  @Test
  void deltaTableStaticCredsEmbedCloudKeysAndOmitDeltaKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            null,
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            CredentialOperation.READ_WRITE,
            s3Creds());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY)
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
  }

  @Test
  void deltaTableUnknownSchemeReturnsEmptyMap() {
    assertThat(
            CredPropsUtil.createDeltaTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                "http://uc",
                null,
                UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
                "hdfs://namenode/tbl",
                CredentialOperation.READ_WRITE,
                s3Creds()))
        .isEmpty();
  }

  @Test
  void s3DeltaTableOriginalImplPreservedWithCredScopedFs() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            true,
            conf,
            "s3",
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            CredentialOperation.READ_WRITE,
            s3Creds());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void deltaTableOriginalImplNotSetWhenCredScopedFsDisabled() {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            CredentialOperation.READ_WRITE,
            s3Creds());

    assertThat(props)
        .doesNotContainKey("fs.s3.impl.original")
        .doesNotContainKey("fs.s3a.impl.original");
  }

  @Test
  void propsBuilderRejectsTableIdAfterDeltaTableIdentifier() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.ucDeltaTableIdentifier(
        UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "s3://bucket/tbl");

    assertThatThrownBy(() -> builder.tableId("table-id"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tableId cannot be set with UC Delta table identifier");
  }

  @Test
  void propsBuilderRejectsDeltaTableIdentifierAfterTableId() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.tableId("table-id");

    assertThatThrownBy(
            () ->
                builder.ucDeltaTableIdentifier(
                    UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "s3://bucket/tbl"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("UC Delta table identifier cannot be set with tableId");
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"));
  }

  private static TemporaryCredentials gcsCreds() {
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken("token"))
        .expirationTime(Long.MAX_VALUE);
  }

  private static TemporaryCredentials abfsCreds() {
    return new TemporaryCredentials()
        .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas"));
  }
}
