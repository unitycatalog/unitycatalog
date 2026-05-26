package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
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
            s3Creds(),
            Map.of());

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
            s3Creds(),
            Map.of());

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
            gcsCreds(),
            Map.of());

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
            abfsCreds(),
            Map.of());

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
            gcsCreds(),
            Map.of());

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
            abfsCreds(),
            Map.of());

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
            s3Creds(),
            Map.of());

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
            s3Creds(),
            Map.of());

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
            gcsCreds(),
            Map.of());

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
            abfsCreds(),
            Map.of());

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
            s3Creds(),
            Map.of());

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
                s3Creds(),
                Map.of()))
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
            s3Creds(),
            Map.of());

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
            s3Creds(),
            Map.of());

    assertThat(props)
        .doesNotContainKey("fs.s3.impl.original")
        .doesNotContainKey("fs.s3a.impl.original");
  }

  // For Delta staging table API.

  @Test
  void s3DeltaStagingTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "s3://bucket/staging",
            s3Creds(),
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/staging")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_TABLE_ID_KEY)
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
  }

  @Test
  void gcsDeltaStagingTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "gs://bucket/staging",
            gcsCreds(),
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "gs://bucket/staging")
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token");
  }

  @Test
  void abfsDeltaStagingTableCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfss",
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "abfss://container@account.dfs.core.windows.net/staging",
            abfsCreds(),
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_LOCATION_KEY,
            "abfss://container@account.dfs.core.windows.net/staging")
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas");
  }

  @Test
  void deltaStagingTableStaticCredsEmbedCloudKeysAndOmitStagingKeys() {
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            null,
            "staging-uuid",
            "s3://bucket/staging",
            s3Creds(),
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY);
  }

  @Test
  void deltaStagingTableUnknownSchemeReturnsEmptyMap() {
    assertThat(
            CredPropsUtil.createDeltaStagingTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                "http://uc",
                null,
                "staging-uuid",
                "hdfs://namenode/staging",
                s3Creds(),
                Map.of()))
        .isEmpty();
  }

  @Test
  void s3DeltaStagingTableOriginalImplPreservedWithCredScopedFs() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            true,
            conf,
            "s3",
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "s3://bucket/staging",
            s3Creds(),
            Map.of());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void propsBuilderRejectsTableIdAfterDeltaTableIdentifier() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.ucDeltaTableIdentifier(
        UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "s3://bucket/tbl");

    assertThatThrownBy(() -> builder.tableId("table-id"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tableId cannot be set");
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
        .hasMessageContaining("UC Delta table identifier cannot be set");
  }

  @Test
  void propsBuilderRejectsTableIdAfterStagingTableId() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.deltaStagingTableId("staging-id", "s3://bucket/staging");

    assertThatThrownBy(() -> builder.tableId("table-id"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tableId cannot be set");
  }

  @Test
  void propsBuilderRejectsDeltaTableIdentifierAfterStagingTableId() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.deltaStagingTableId("staging-id", "s3://bucket/staging");

    assertThatThrownBy(
            () ->
                builder.ucDeltaTableIdentifier(
                    UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "s3://bucket/tbl"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("UC Delta table identifier cannot be set");
  }

  @Test
  void propsBuilderRejectsStagingTableIdAfterTableId() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.tableId("table-id");

    assertThatThrownBy(() -> builder.deltaStagingTableId("staging-id", "s3://bucket/staging"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("deltaStagingTableId cannot be set");
  }

  @Test
  void propsBuilderRejectsStagingTableIdAfterDeltaTableIdentifier() {
    CredPropsUtil.S3PropsBuilder builder =
        new CredPropsUtil.S3PropsBuilder(false, new Configuration(false));
    builder.ucDeltaTableIdentifier(
        UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "s3://bucket/tbl");

    assertThatThrownBy(() -> builder.deltaStagingTableId("staging-id", "s3://bucket/staging"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("deltaStagingTableId cannot be set");
  }

  // UC REST table and path credential props.

  @Test
  void s3TableRenewalCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            tokenProvider(),
            "tid",
            TableOperation.READ_WRITE,
            s3Creds(),
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_URI_KEY, "http://uc")
        .containsEntry(UCHadoopConfConstants.UC_AUTH_TYPE, "static")
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsKey(UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY)
        .containsEntry(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid")
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st")
        .doesNotContainKey("fs.s3a.access.key");
  }

  @Test
  void s3TableStaticCredsHaveExpectedKeys() {
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
            s3Creds(),
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
  }

  @Test
  void gcsTableRenewalCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            "http://uc",
            tokenProvider(),
            "tid",
            TableOperation.READ,
            gcsCreds(),
            Map.of());

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token")
        .containsEntry(
            UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
            String.valueOf(Long.MAX_VALUE))
        .doesNotContainKey("fs.gs.auth.access.token.credential");
  }

  @Test
  void abfsTableRenewalCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfs",
            "http://uc",
            tokenProvider(),
            "tid",
            TableOperation.READ_WRITE,
            abfsCreds(),
            Map.of());

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas")
        .doesNotContainKey("fs.azure.sas.fixed.token");
  }

  @Test
  void s3PathRenewalCredsHaveExpectedKeys() {
    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            "http://uc",
            tokenProvider(),
            "s3://bucket/key",
            io.unitycatalog.client.model.PathOperation.PATH_READ,
            s3Creds(),
            Map.of());

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_PATH_KEY, "s3://bucket/key")
        .containsEntry(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "PATH_READ")
        .doesNotContainKey(UCHadoopConfConstants.UC_TABLE_ID_KEY);
  }

  @Test
  void returnedTableCredMapIsUnmodifiable() {
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
            s3Creds(),
            Map.of());

    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void unknownSchemeReturnsEmptyTableCredMap() {
    assertThat(
            CredPropsUtil.createTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                "http://uc",
                null,
                "tid",
                TableOperation.READ,
                s3Creds(),
                Map.of()))
        .isEmpty();
  }

  @Test
  void unknownSchemeReturnsEmptyPathCredMap() {
    assertThat(
            CredPropsUtil.createPathCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                "http://uc",
                null,
                "hdfs://nn/key",
                io.unitycatalog.client.model.PathOperation.PATH_READ,
                s3Creds(),
                Map.of()))
        .isEmpty();
  }

  // Fetch-method orchestration tests: capture the req-conf assembled by each fetch* method
  // before it would hit the wire, and assert the resulting props are credential-bearing.

  @AfterEach
  void resetFactory() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
  }

  @Test
  void fetchTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<Configuration> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, conf) -> {
          captured.set(conf);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.fetchTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(captured.get().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    assertThat(captured.get().get(UCHadoopConfConstants.UC_TABLE_ID_KEY)).isEqualTo("tid");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY))
        .isEqualTo("READ_WRITE");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void fetchDeltaTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<Configuration> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, conf) -> {
          captured.set(conf);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.fetchDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tab"),
            "s3://bucket/key",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    assertThat(
            captured
                .get()
                .getBoolean(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, false))
        .isTrue();
    assertThat(captured.get().get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY)).isEqualTo("READ");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY)).isEqualTo("cat");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY)).isEqualTo("sch");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY)).isEqualTo("tab");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY))
        .isEqualTo("s3://bucket/key");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void fetchPathCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<Configuration> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, conf) -> {
          captured.set(conf);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.fetchPathCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "s3://bucket/key",
            UCCredentialHadoopConfs.PathOperation.PATH_CREATE_TABLE,
            Map.of());

    assertThat(captured.get().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    assertThat(captured.get().get(UCHadoopConfConstants.UC_PATH_KEY)).isEqualTo("s3://bucket/key");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_PATH_OPERATION_KEY))
        .isEqualTo("PATH_CREATE_TABLE");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void fetchDeltaStagingTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<Configuration> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, conf) -> {
          captured.set(conf);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.fetchDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "s3://bucket/staging",
            Map.of());

    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY))
        .isEqualTo("staging-uuid");
    assertThat(captured.get().get(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY))
        .isEqualTo("s3://bucket/staging");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void fetchTableCredPropsIncludesAppVersionProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, conf) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> appVersions = Map.of("Spark", "4.0.0", "Delta", "3.3.0");

    Map<String, String> props =
        CredPropsUtil.fetchTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            appVersions);

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Spark", "4.0.0")
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Delta", "3.3.0")
        .containsKey(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  private static GenericCredentialFetcher mockGenericCredentialFetcher(TemporaryCredentials creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredential()).thenReturn(new GenericCredential(creds));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
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
