package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
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
            "io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider")
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
            "io.unitycatalog.spark.auth.storage.GcsVendedTokenProvider")
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
            "io.unitycatalog.spark.auth.storage.AbfsVendedTokenProvider")
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
}
