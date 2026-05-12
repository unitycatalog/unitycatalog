package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class UCCredentialHadoopConfsTest {

  private static final String S3A_FS = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String GCS_FS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String ABFS_FS = "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem";
  private static final String ABFSS_FS = "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem";

  @Test
  void s3TableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st");
  }

  @Test
  void s3TableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3.impl.original", S3A_FS)
        .containsEntry("fs.s3a.impl.original", S3A_FS);
  }

  @Test
  void s3TableWithCustomFsImpl() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3.impl", "com.example.Custom");
    conf.set("fs.s3a.impl", "com.example.Custom");

    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(true)
            .hadoopConf(conf)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.s3.impl.original", "com.example.Custom")
        .containsEntry("fs.s3a.impl.original", "com.example.Custom");
  }

  @Test
  void s3PathCredentials() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForPath("s3://bucket/path", PathOperation.PATH_READ);

    assertThat(props).containsEntry("fs.s3a.access.key", "ak");
  }

  @Test
  void gsTableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("gs")
            .initialCredentials(gcsCreds())
            .buildForTable("tid", TableOperation.READ);

    assertThat(props)
        .containsEntry("fs.gs.auth.access.token.credential", "gcs-token")
        .containsEntry("fs.gs.auth.access.token.expiration", String.valueOf(Long.MAX_VALUE));
  }

  @Test
  void gsTableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("gs")
            .initialCredentials(gcsCreds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).containsEntry("fs.gs.impl.original", GCS_FS);
  }

  @Test
  void abfsTableStaticCredentials() {
    Map<String, String> props =
        staticBuilder("abfs")
            .initialCredentials(abfsCreds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.azure.sas.fixed.token", "sas-token")
        .containsEntry("fs.azure.account.auth.type", "SAS");
  }

  @Test
  void abfssTableWithCredScopedFs() {
    Map<String, String> props =
        staticBuilder("abfss")
            .initialCredentials(abfsCreds())
            .enableCredentialScopedFs(true)
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.abfs.impl.original", ABFS_FS)
        .containsEntry("fs.abfss.impl.original", ABFSS_FS);
  }

  @Test
  void s3TableWithCredentialRenewal() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.unitycatalog.uri", "http://uc")
        .containsEntry("fs.unitycatalog.auth.type", "static")
        .containsEntry("fs.unitycatalog.auth.token", "test-token")
        .containsKey("fs.unitycatalog.credentials.uid")
        .containsEntry("fs.unitycatalog.credentials.type", "table")
        .containsEntry("fs.unitycatalog.table.id", "tid")
        .containsEntry("fs.unitycatalog.table.operation", TableOperation.READ_WRITE.getValue())
        .containsEntry(
            "fs.s3a.aws.credentials.provider",
            "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider")
        .containsEntry("fs.s3a.init.access.key", "ak")
        .containsEntry("fs.s3a.init.secret.key", "sk")
        .containsEntry("fs.s3a.init.session.token", "st")
        .doesNotContainKey("fs.s3a.init.credential.expired.time")
        .doesNotContainKey("fs.s3a.access.key");
  }

  @Test
  void s3RenewalWithExpirationTime() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialCredentials(s3Creds().expirationTime(9999L))
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).containsEntry("fs.s3a.init.credential.expired.time", "9999");
  }

  @Test
  void gsTableWithCredentialRenewal() {
    Map<String, String> props =
        renewalBuilder("gs")
            .initialCredentials(gcsCreds())
            .buildForTable("tid", TableOperation.READ);

    assertThat(props)
        .containsEntry("fs.unitycatalog.uri", "http://uc")
        .containsEntry("fs.unitycatalog.auth.type", "static")
        .containsEntry("fs.unitycatalog.auth.token", "test-token")
        .containsKey("fs.unitycatalog.credentials.uid")
        .containsEntry("fs.unitycatalog.credentials.type", "table")
        .containsEntry("fs.unitycatalog.table.id", "tid")
        .containsEntry("fs.unitycatalog.table.operation", TableOperation.READ.getValue())
        .containsEntry(
            "fs.gs.auth.access.token.provider",
            "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider")
        .containsEntry("fs.gs.init.oauth.token", "gcs-token")
        .containsEntry("fs.gs.init.oauth.token.expiration.time", String.valueOf(Long.MAX_VALUE))
        .doesNotContainKey("fs.gs.auth.access.token.credential");
  }

  @Test
  void abfsTableWithCredentialRenewal() {
    Map<String, String> props =
        renewalBuilder("abfs")
            .initialCredentials(abfsCreds())
            .buildForTable("tid", TableOperation.READ_WRITE);

    assertThat(props)
        .containsEntry("fs.unitycatalog.uri", "http://uc")
        .containsEntry("fs.unitycatalog.auth.type", "static")
        .containsEntry("fs.unitycatalog.auth.token", "test-token")
        .containsKey("fs.unitycatalog.credentials.uid")
        .containsEntry("fs.unitycatalog.credentials.type", "table")
        .containsEntry("fs.unitycatalog.table.id", "tid")
        .containsEntry("fs.unitycatalog.table.operation", TableOperation.READ_WRITE.getValue())
        .containsEntry(
            "fs.azure.sas.token.provider.type",
            "io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider")
        .containsEntry("fs.azure.init.sas.token", "sas-token")
        .doesNotContainKey("fs.azure.init.sas.token.expired.time")
        .doesNotContainKey("fs.azure.sas.fixed.token");
  }

  @Test
  void s3PathWithCredentialRenewal() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialCredentials(s3Creds())
            .buildForPath("s3://bucket/key", PathOperation.PATH_READ);

    assertThat(props)
        .containsEntry("fs.unitycatalog.uri", "http://uc")
        .containsEntry("fs.unitycatalog.credentials.type", "path")
        .containsEntry("fs.unitycatalog.path", "s3://bucket/key")
        .containsEntry("fs.unitycatalog.path.operation", PathOperation.PATH_READ.getValue())
        .doesNotContainKey("fs.unitycatalog.table.id");
  }

  @Test
  void engineVersionsAreIncludedInTableCredentialProps() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialCredentials(s3Creds())
            .addEngineVersions(Map.of("Spark", "4.0.0", "Delta", "3.3.0"))
            .buildForTable("tid", TableOperation.READ);

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Spark", "4.0.0")
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Delta", "3.3.0");
  }

  @Test
  void engineVersionsAreIncludedInPathCredentialProps() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialCredentials(s3Creds())
            .addEngineVersions(Map.of("Spark", "4.0.0"))
            .buildForPath("s3://bucket/key", PathOperation.PATH_READ);

    assertThat(props)
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Spark", "4.0.0")
        .containsEntry("fs.unitycatalog.path", "s3://bucket/key");
  }

  @Test
  void emptyEngineVersionThrows() {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .addEngineVersions(Map.of("Spark", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Spark");
  }

  @Test
  void oauthProviderConfigsArePropagated() {
    TokenProvider oauth = oauthProvider();
    assertOAuthProps(
        renewalBuilder("s3", oauth)
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ));
    assertOAuthProps(
        renewalBuilder("gs", oauth)
            .initialCredentials(gcsCreds())
            .buildForTable("tid", TableOperation.READ));
    assertOAuthProps(
        renewalBuilder("abfs", oauth)
            .initialCredentials(abfsCreds())
            .buildForTable("tid", TableOperation.READ));
  }

  @Test
  void unknownSchemeReturnsEmptyMap() {
    Map<String, String> props =
        staticBuilder("hdfs")
            .initialCredentials(s3Creds())
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).isEmpty();
  }

  @Test
  void unknownTableSchemeWithEngineVersionsReturnsEmptyMap() {
    Map<String, String> props =
        staticBuilder("hdfs")
            .initialCredentials(s3Creds())
            .addEngineVersions(Map.of("Spark", "4.0.0"))
            .buildForTable("tid", TableOperation.READ);

    assertThat(props).isEmpty();
  }

  @Test
  void unknownPathSchemeWithEngineVersionsReturnsEmptyMap() {
    Map<String, String> props =
        staticBuilder("hdfs")
            .initialCredentials(s3Creds())
            .addEngineVersions(Map.of("Spark", "4.0.0"))
            .buildForPath("hdfs://bucket/key", PathOperation.PATH_READ);

    assertThat(props).isEmpty();
  }

  @Test
  void credScopedFsDisabledNoOriginalKeys() {
    Map<String, String> props =
        staticBuilder("s3")
            .initialCredentials(s3Creds())
            .enableCredentialScopedFs(false)
            .buildForTable("tid", TableOperation.READ);

    assertThat(props)
        .doesNotContainKey("fs.s3.impl.original")
        .doesNotContainKey("fs.s3a.impl.original");
  }

  @Test
  void returnedMapIsUnmodifiable() {
    Map<String, String> props =
        staticBuilder("s3").initialCredentials(s3Creds()).buildForTable("tid", TableOperation.READ);

    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void missingCatalogUriThrows() {
    assertThatThrownBy(() -> UCCredentialHadoopConfs.builder(null, "s3"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalogUri");
  }

  @Test
  void missingSchemeThrows() {
    assertThatThrownBy(() -> UCCredentialHadoopConfs.builder("http://uc", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scheme");
  }

  @Test
  void missingInitialCredentialsThrows() {
    assertThatThrownBy(() -> staticBuilder("s3").buildForTable("tid", TableOperation.READ))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initialCredentials");
  }

  @Test
  void missingTokenProviderWithRenewalThrows() {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .initialCredentials(s3Creds())
                    .buildForTable("tid", TableOperation.READ))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tokenProvider");
  }

  private static UCCredentialHadoopConfs.Builder staticBuilder(String scheme) {
    return UCCredentialHadoopConfs.builder("http://uc", scheme).enableCredentialRenewal(false);
  }

  private static UCCredentialHadoopConfs.Builder renewalBuilder(String scheme) {
    return renewalBuilder(
        scheme, TokenProvider.create(Map.of("type", "static", "token", "test-token")));
  }

  private static UCCredentialHadoopConfs.Builder renewalBuilder(
      String scheme, TokenProvider provider) {
    return UCCredentialHadoopConfs.builder("http://uc", scheme).tokenProvider(provider);
  }

  private static TokenProvider oauthProvider() {
    return TokenProvider.create(
        Map.of(
            "type", "oauth",
            "oauth.uri", "https://auth.example.com/token",
            "oauth.clientId", "my-client-id",
            "oauth.clientSecret", "my-secret"));
  }

  private static void assertOAuthProps(Map<String, String> props) {
    assertThat(props)
        .containsEntry("fs.unitycatalog.auth.type", "oauth")
        .containsEntry("fs.unitycatalog.auth.oauth.uri", "https://auth.example.com/token")
        .containsEntry("fs.unitycatalog.auth.oauth.clientId", "my-client-id")
        .containsEntry("fs.unitycatalog.auth.oauth.clientSecret", "my-secret");
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"));
  }

  private static TemporaryCredentials gcsCreds() {
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken("gcs-token"))
        .expirationTime(Long.MAX_VALUE);
  }

  private static TemporaryCredentials abfsCreds() {
    return new TemporaryCredentials()
        .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token"));
  }

  private static StorageCredential deltaS3Credential() {
    return new StorageCredential()
        .prefix("s3://bucket/table")
        .operation(CredentialOperation.READ_WRITE)
        .config(
            new StorageCredentialConfig()
                .s3AccessKeyId("ak")
                .s3SecretAccessKey("sk")
                .s3SessionToken("st"));
  }

  private static StorageCredential deltaGcsCredential() {
    return new StorageCredential()
        .prefix("gs://bucket/table")
        .operation(CredentialOperation.READ)
        .expirationTimeMs(4242L)
        .config(new StorageCredentialConfig().gcsOauthToken("gcs-oauth-token"));
  }

  private static StorageCredential deltaAzureCredential() {
    return new StorageCredential()
        .prefix("abfss://container@account.dfs.core.windows.net/table")
        .operation(CredentialOperation.READ_WRITE)
        .expirationTimeMs(5151L)
        .config(new StorageCredentialConfig().azureSasToken("sas-token"));
  }

  @Test
  void s3DeltaTableBuildsExpectedKeys() {
    Map<String, String> props =
        renewalBuilder("s3")
            .initialStorageCredential(deltaS3Credential())
            .buildForTable(
                "catalog", "schema", "table", CredentialOperation.READ_WRITE, "s3://bucket/table");

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, Boolean.TRUE.toString())
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .doesNotContainKey(UCHadoopConfConstants.UC_TABLE_ID_KEY)
        .containsEntry(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "catalog")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "schema")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "table")
        .containsEntry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/table")
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st");
  }

  @Test
  void deltaTableBuildsExpectedCloudSpecificInitialCredentialKeys() {
    Map<String, String> gcsProps =
        renewalBuilder("gs")
            .initialStorageCredential(deltaGcsCredential())
            .buildForTable(
                "catalog", "schema", "table", CredentialOperation.READ, "gs://bucket/table");
    assertThat(gcsProps)
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ")
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "gcs-oauth-token")
        .containsEntry(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME, "4242");

    Map<String, String> azureProps =
        renewalBuilder("abfss")
            .initialStorageCredential(deltaAzureCredential())
            .buildForTable(
                "catalog",
                "schema",
                "table",
                CredentialOperation.READ_WRITE,
                "abfss://container@account.dfs.core.windows.net/table");
    assertThat(azureProps)
        .containsEntry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE")
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, "sas-token")
        .containsEntry(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME, "5151");
  }

  @Test
  void deltaTableBuildRejectsMultiCloudInitialStorageCredential() {
    StorageCredential credential =
        new StorageCredential()
            .prefix("gs://bucket/table")
            .operation(CredentialOperation.READ)
            .config(
                new StorageCredentialConfig()
                    .gcsOauthToken("gcs-oauth-token")
                    .azureSasToken("sas-token"));

    assertThatThrownBy(
            () ->
                renewalBuilder("gs")
                    .initialStorageCredential(credential)
                    .buildForTable(
                        "catalog", "schema", "table", CredentialOperation.READ, "gs://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain exactly one cloud credential config");
  }

  @Test
  void initialStorageCredentialRequiredForDeltaApi() {
    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .buildForTable(
                        "catalog", "schema", "table", CredentialOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initialStorageCredential is required");
  }

  @Test
  void cannotSetBothInitialCredentialAndInitialStorageCredential() {
    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialCredentials(s3Creds())
                    .initialStorageCredential(deltaS3Credential()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot also set initialStorageCredential");

    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .initialCredentials(s3Creds()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot also set initialCredentials");
  }

  @Test
  void deltaTableBuildRejectsInitialCredentials() {
    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialCredentials(s3Creds())
                    .buildForTable(
                        "catalog", "schema", "table", CredentialOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initialStorageCredential is required");
  }

  @Test
  void ucBuilderRejectsInitialStorageCredential() {
    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable("tid", TableOperation.READ_WRITE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initialCredentials is required");
  }

  @Test
  void deltaTableBuildRejectsMissingFields() {
    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable(
                        null,
                        "schema",
                        "table",
                        CredentialOperation.READ_WRITE,
                        "s3://bucket/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog is required");

    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable(
                        "catalog",
                        null,
                        "table",
                        CredentialOperation.READ_WRITE,
                        "s3://bucket/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema is required");

    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable(
                        "catalog",
                        "schema",
                        null,
                        CredentialOperation.READ_WRITE,
                        "s3://bucket/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("table is required");

    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable("catalog", "schema", "table", null, "s3://bucket/table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("operation is required");

    assertThatThrownBy(
            () ->
                renewalBuilder("s3")
                    .initialStorageCredential(deltaS3Credential())
                    .buildForTable(
                        "catalog", "schema", "table", CredentialOperation.READ_WRITE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("location is required");
  }
}
