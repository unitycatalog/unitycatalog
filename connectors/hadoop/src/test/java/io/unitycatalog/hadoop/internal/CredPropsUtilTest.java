package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  @BeforeEach
  void clearCredentialCache() {
    CredPropsUtil.initialCredCache.clear();
  }

  @Test
  void s3OriginalImplPreservedFromExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void s3DefaultOriginalImplWhenNotInExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.s3.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
    assertThat(props.get("fs.s3a.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
  }

  @Test
  void gsOriginalImplPreservedFromExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Configuration conf = new Configuration(false);
    conf.set("fs.gs.impl", CUSTOM_GS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.gs.impl.original")).isEqualTo(CUSTOM_GS_IMPL);
  }

  @Test
  void abfsOriginalImplPreservedFromExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(abfsCreds());
    Configuration conf = new Configuration(false);
    conf.set("fs.abfs.impl", CUSTOM_ABFS_IMPL);
    conf.set("fs.abfss.impl", CUSTOM_ABFSS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            conf,
            "abfs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.abfs.impl.original")).isEqualTo(CUSTOM_ABFS_IMPL);
    assertThat(props.get("fs.abfss.impl.original")).isEqualTo(CUSTOM_ABFSS_IMPL);
  }

  @Test
  void gsDefaultOriginalImplWhenNotInExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.gs.impl.original"))
        .isEqualTo("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
  }

  @Test
  void abfsDefaultOriginalImplWhenNotInExistingProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(abfsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            new Configuration(false),
            "abfs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.abfs.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    assertThat(props.get("fs.abfss.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
  }

  @Test
  void originalImplNotSetWhenCredScopedFsDisabled() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).doesNotContainKey("fs.s3.impl.original");
    assertThat(props).doesNotContainKey("fs.s3a.impl.original");
  }

  // For unitycatalog delta table API.

  @Test
  void s3DeltaTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
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
  void gcsDeltaTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "gs://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ,
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
  void abfsDeltaTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(abfsCreds());
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfss",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "abfss://container@account.dfs.core.windows.net/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
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
  void deltaTableStaticCredsEmbedCloudKeysAndOmitDeltaKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY)
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
  }

  @Test
  void deltaTableUnknownSchemeReturnsEmptyMap() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    assertThat(
            CredPropsUtil.createDeltaTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                null,
                "http://uc",
                tokenProvider(),
                UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
                "hdfs://namenode/tbl",
                UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                Map.of()))
        .isEmpty();
  }

  @Test
  void s3DeltaTableOriginalImplPreservedWithCredScopedFs() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            true,
            conf,
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void deltaTableOriginalImplNotSetWhenCredScopedFsDisabled() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props)
        .doesNotContainKey("fs.s3.impl.original")
        .doesNotContainKey("fs.s3a.impl.original");
  }

  // For Delta staging table API.

  @Test
  void s3DeltaStagingTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
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

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "s3://bucket/staging")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_TABLE_ID_KEY)
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
  }

  @Test
  void gcsDeltaStagingTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "gs://bucket/staging",
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "gs://bucket/staging")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token");
  }

  @Test
  void abfsDeltaStagingTableCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(abfsCreds());
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfss",
            null,
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "abfss://container@account.dfs.core.windows.net/staging",
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "staging-uuid")
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY,
            "abfss://container@account.dfs.core.windows.net/staging")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true")
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas");
  }

  @Test
  void deltaStagingTableStaticCredsEmbedCloudKeysAndOmitStagingKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "s3://bucket/staging",
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY);
  }

  @Test
  void deltaStagingTableUnknownSchemeReturnsEmptyMap() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    assertThat(
            CredPropsUtil.createDeltaStagingTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                null,
                "http://uc",
                tokenProvider(),
                "staging-uuid",
                "hdfs://namenode/staging",
                Map.of()))
        .isEmpty();
  }

  @Test
  void s3DeltaStagingTableOriginalImplPreservedWithCredScopedFs() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", CUSTOM_S3_IMPL);
    conf.set("fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
            true,
            true,
            conf,
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "staging-uuid",
            "s3://bucket/staging",
            Map.of());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  // UC REST table and path credential props.

  @Test
  void s3TableRenewalCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
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

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_URI_KEY, "http://uc")
        .containsEntry(UCHadoopConfConstants.UC_AUTH_TYPE, "static")
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid")
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st")
        .doesNotContainKey("fs.s3a.access.key");
  }

  @Test
  void s3TableStaticCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
  }

  @Test
  void gcsTableRenewalCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ,
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
  void abfsTableRenewalCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(abfsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "abfs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas")
        .doesNotContainKey("fs.azure.sas.fixed.token");
  }

  @Test
  void s3PathRenewalCredsHaveExpectedKeys() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "s3://bucket/key",
            UCCredentialHadoopConfs.PathOperation.PATH_READ,
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
  void returnedTableCredMapIsUnmodifiable() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void unknownSchemeReturnsEmptyTableCredMap() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    assertThat(
            CredPropsUtil.createTableCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                null,
                "http://uc",
                tokenProvider(),
                "tid",
                UCCredentialHadoopConfs.TableOperation.READ,
                Map.of()))
        .isEmpty();
  }

  @Test
  void unknownSchemeReturnsEmptyPathCredMap() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    assertThat(
            CredPropsUtil.createPathCredProps(
                false,
                false,
                new Configuration(false),
                "hdfs",
                null,
                "http://uc",
                tokenProvider(),
                "hdfs://nn/key",
                UCCredentialHadoopConfs.PathOperation.PATH_READ,
                Map.of()))
        .isEmpty();
  }

  // Fetch-method orchestration tests: capture the req-conf assembled by each fetch* method
  // before it would hit the wire, and assert the resulting props are credential-bearing.

  @AfterEach
  void resetFactory() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
  }

  // Driver-side credential cache: different queries reuse the same credential for the same credId.

  @Test
  void sameCredIdReusesCachedCredentialAcrossQueries() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> first = createTableCredProps(new Configuration(false));
    Map<String, String> second = createTableCredProps(new Configuration(false));

    assertThat(fetches.get()).isEqualTo(1);
    assertThat(first).isEqualTo(second).containsEntry("fs.s3a.access.key", "ak");
  }

  @Test
  void differentCredIdsAreCachedSeparately() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    createTableCredProps(new Configuration(false), "tidA");
    createTableCredProps(new Configuration(false), "tidB");

    assertThat(fetches.get()).isEqualTo(2);
  }

  @Test
  void sameTableDifferentAuthConfigsAreCachedSeparately() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(
              s3CredsExpiringAt(
                  String.valueOf(fetches.get()), System.currentTimeMillis() + 60_000));
        };

    TokenProvider tenantA = TokenProvider.create(Map.of("type", "static", "token", "tenant-a"));
    TokenProvider tenantB = TokenProvider.create(Map.of("type", "static", "token", "tenant-b"));

    createTableCredProps(new Configuration(false), "shared-table", tenantA);
    createTableCredProps(new Configuration(false), "shared-table", tenantB);

    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * The credential-context id folds in {@code catalogUri}, so the same resource accessed via two
   * different catalogs with the same auth must not share a cached credential. Covers every {@link
   * CredId} variant produced by the {@code create*CredProps} entry points.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("credIdVariants")
  void sameResourceDifferentCatalogUriCachedSeparately(String variant, CredPropsFetch fetch)
      throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    fetch.fetch("http://uc-a");
    fetch.fetch("http://uc-b");

    assertThat(fetches.get()).isEqualTo(2);
  }

  @Test
  void credPropsIncludeCredContextIdFromContext() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    TokenProvider provider = TokenProvider.create(Map.of("type", "static", "token", "tenant-a"));
    String expectedContextId = CredPropsUtil.credContextId("http://uc", "s3", provider);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            provider,
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, expectedContextId);
  }

  @Test
  void credContextIdVariesByCatalogUriSchemeAndAuth() {
    TokenProvider auth = tokenProvider();
    String base = CredPropsUtil.credContextId("http://uc", "s3", auth);

    assertThat(CredPropsUtil.credContextId("http://uc", "s3", auth)).isEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc-2", "s3", auth)).isNotEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc", "gs", auth)).isNotEqualTo(base);
    assertThat(
            CredPropsUtil.credContextId(
                "http://uc", "s3", TokenProvider.create(Map.of("type", "static", "token", "x"))))
        .isNotEqualTo(base);
  }

  @Test
  void cacheDisabledFetchesForEveryQuery() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };
    Configuration conf = new Configuration(false);
    conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);

    createTableCredProps(conf);
    createTableCredProps(conf);

    assertThat(fetches.get()).isEqualTo(2);
  }

  @Test
  void expiredCachedCredentialIsRefetched() throws Exception {
    String clockName = UUID.randomUUID().toString();
    Clock clock = Clock.getManualClock(clockName);
    try {
      GenericCredential cred1 = s3CredsExpiringAt("1", clock.now().toEpochMilli() + 2000L);
      GenericCredential cred2 = s3CredsExpiringAt("2", clock.now().toEpochMilli() + 20000L);
      AtomicInteger fetches = new AtomicInteger();
      CredPropsUtil.genericCredFetcherFactory =
          (apiClient, credId) ->
              mockGenericCredentialFetcher(fetches.getAndIncrement() == 0 ? cred1 : cred2);

      Configuration conf = new Configuration(false);
      conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
      conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

      // 1st query fetches cred1 and caches it.
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st1");
      // 2nd query reuses cred1 while it is still valid.
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st1");
      assertThat(fetches.get()).isEqualTo(1);

      // Advance the clock so cred1 is within the renewal lead time; the next query refetches cred2.
      clock.sleep(Duration.ofMillis(1500));
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st2");
      assertThat(fetches.get()).isEqualTo(2);
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  private static Map<String, String> createTableCredProps(Configuration conf) throws Exception {
    return createTableCredProps(conf, "tid");
  }

  private static Map<String, String> createTableCredProps(Configuration conf, String tableId)
      throws Exception {
    return createTableCredProps(conf, tableId, tokenProvider());
  }

  private static Map<String, String> createTableCredProps(
      Configuration conf, String tableId, TokenProvider tokenProvider) throws Exception {
    return createTableCredProps(conf, "http://uc", tableId, tokenProvider);
  }

  private static Map<String, String> createTableCredProps(
      Configuration conf, String catalogUri, String tableId, TokenProvider tokenProvider)
      throws Exception {
    return CredPropsUtil.createTableCredProps(
        false,
        false,
        conf,
        "s3",
        null,
        catalogUri,
        tokenProvider,
        tableId,
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  /** Fetches credential props for one fixed resource + auth against the given catalog. */
  @FunctionalInterface
  private interface CredPropsFetch {
    void fetch(String catalogUri) throws Exception;
  }

  private static Stream<Arguments> credIdVariants() {
    return Stream.of(
        Arguments.of(
            "TableCredId",
            (CredPropsFetch)
                catalogUri ->
                    createTableCredProps(
                        new Configuration(false), catalogUri, "shared-table", tokenProvider())),
        Arguments.of(
            "PathCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createPathCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        "s3://bucket/shared",
                        UCCredentialHadoopConfs.PathOperation.PATH_READ_WRITE,
                        Map.of())),
        Arguments.of(
            "DeltaTableCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createDeltaTableCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
                        "s3://bucket/tbl",
                        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                        Map.of())),
        Arguments.of(
            "DeltaStagingTableCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createDeltaStagingTableCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        "staging-uuid",
                        "s3://bucket/staging",
                        Map.of())));
  }

  @Test
  void createTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<CredId> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          captured.set(credId);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
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

    assertThat(captured.get().props().get(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY))
        .isEqualTo(CredPropsUtil.credContextId("http://uc", "s3", tokenProvider()));
    assertThat(captured.get().props().get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    assertThat(captured.get().props().get(UCHadoopConfConstants.UC_TABLE_ID_KEY)).isEqualTo("tid");
    assertThat(captured.get().props().get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY))
        .isEqualTo("READ_WRITE");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void createDeltaTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<CredId> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          captured.set(credId);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
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

    Map<String, String> reqProps = captured.get().props();
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY))
        .isEqualTo("true");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY)).isEqualTo("READ");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY)).isEqualTo("cat");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY)).isEqualTo("sch");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY)).isEqualTo("tab");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY))
        .isEqualTo("s3://bucket/key");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void createPathCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<CredId> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          captured.set(credId);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
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

    Map<String, String> reqProps = captured.get().props();
    assertThat(reqProps.get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY))
        .isEqualTo(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    assertThat(reqProps.get(UCHadoopConfConstants.UC_PATH_KEY)).isEqualTo("s3://bucket/key");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_PATH_OPERATION_KEY))
        .isEqualTo("PATH_CREATE_TABLE");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void createDeltaStagingTableCredPropsAssemblesReqConfAndReturnsCredProps() throws Exception {
    AtomicReference<CredId> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          captured.set(credId);
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> props =
        CredPropsUtil.createDeltaStagingTableCredProps(
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

    Map<String, String> reqProps = captured.get().props();
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY))
        .isEqualTo("true");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY))
        .isEqualTo("staging-uuid");
    assertThat(reqProps.get(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY))
        .isEqualTo("s3://bucket/staging");
    assertThat(props).containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak");
  }

  @Test
  void createTableCredPropsIncludesAppVersionProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(s3Creds());
    Map<String, String> appVersions = Map.of("Spark", "4.0.0", "Delta", "3.3.0");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
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

  // GCS conflict-check setting propagation.

  @Test
  void gcsConflictCheckDefaultsFalse() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry("fs.gs.create.items.conflict.check.enable", "false");
  }

  @Test
  void gcsConflictCheckRespectsUserOverrideToTrue() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Configuration conf = new Configuration(false);
    conf.set("fs.gs.create.items.conflict.check.enable", "true");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            conf,
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry("fs.gs.create.items.conflict.check.enable", "true");
  }

  @Test
  void gcsConflictCheckDefaultWithRenewalEnabled() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "tid",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry("fs.gs.create.items.conflict.check.enable", "false");
  }

  @Test
  void gcsConflictCheckDefaultPathAndDeltaCredProps() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(gcsCreds());
    Map<String, String> pathProps =
        CredPropsUtil.createPathCredProps(
            false,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            "gs://bucket/key",
            UCCredentialHadoopConfs.PathOperation.PATH_READ,
            Map.of());

    assertThat(pathProps).containsEntry("fs.gs.create.items.conflict.check.enable", "false");

    Map<String, String> deltaProps =
        CredPropsUtil.createDeltaTableCredProps(
            false,
            false,
            new Configuration(false),
            "gs",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "gs://bucket/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(deltaProps).containsEntry("fs.gs.create.items.conflict.check.enable", "false");
  }

  private static GenericCredentialFetcher mockGenericCredentialFetcher(GenericCredential creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredential()).thenReturn(creds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  private static GenericCredential s3Creds() {
    return GenericCredential.forAws("ak", "sk", "st", null);
  }

  private static GenericCredential s3CredsExpiringAt(String id, long expirationMillis) {
    return GenericCredential.forAws("ak" + id, "sk" + id, "st" + id, expirationMillis);
  }

  private static GenericCredential gcsCreds() {
    return GenericCredential.forGcs("token", Long.MAX_VALUE);
  }

  private static GenericCredential abfsCreds() {
    return GenericCredential.forAzure("sas", null);
  }
}
