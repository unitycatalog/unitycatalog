package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.unitycatalog.server.delta.model.CredentialOperation;
import io.unitycatalog.server.delta.model.CredentialsResponse;
import io.unitycatalog.server.delta.model.StorageCredential;
import io.unitycatalog.server.delta.model.StorageCredentialConfig;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import org.junit.jupiter.api.Test;

public class DeltaCredentialsMapperTest {

  @Test
  public void testAwsCredentials() {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA123")
                    .secretAccessKey("secret")
                    .sessionToken("token"))
            .expirationTime(1700000000000L);

    CredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(
            "s3://bucket/path", uc, CredentialOperation.READ);

    assertThat(resp.getStorageCredentials()).hasSize(1);
    StorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getPrefix()).isEqualTo("s3://bucket/path");
    assertThat(sc.getOperation()).isEqualTo(CredentialOperation.READ);
    assertThat(sc.getExpirationTimeMs()).isEqualTo(1700000000000L);
    StorageCredentialConfig config = sc.getConfig();
    assertThat(config.getS3AccessKeyId()).isEqualTo("AKIA123");
    assertThat(config.getS3SecretAccessKey()).isEqualTo("secret");
    assertThat(config.getS3SessionToken()).isEqualTo("token");
    assertThat(config.getAzureSasToken()).isNull();
    assertThat(config.getGcsOauthToken()).isNull();
  }

  @Test
  public void testAzureCredentials() {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sv=..."))
            .expirationTime(1700000000000L);

    CredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(
            "abfss://container@acct.dfs.core.windows.net/path", uc, CredentialOperation.READ_WRITE);

    StorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getOperation()).isEqualTo(CredentialOperation.READ_WRITE);
    assertThat(sc.getConfig().getAzureSasToken()).isEqualTo("sv=...");
    assertThat(sc.getConfig().getS3AccessKeyId()).isNull();
  }

  @Test
  public void testGcpCredentials() {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .gcpOauthToken(new GcpOauthToken().oauthToken("ya29..."))
            .expirationTime(1700000000000L);

    CredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(
            "gs://bucket/path", uc, CredentialOperation.READ);

    StorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getConfig().getGcsOauthToken()).isEqualTo("ya29...");
    assertThat(sc.getConfig().getS3AccessKeyId()).isNull();
  }

  @Test
  public void testPartialAwsCredentials() {
    // Only access key + secret, no session token (permanent credentials case)
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("AKIA123").secretAccessKey("secret"))
            .expirationTime(1700000000000L);

    StorageCredential sc =
        DeltaCredentialsMapper.toCredentialsResponse(
                "s3://bucket/path", uc, CredentialOperation.READ)
            .getStorageCredentials()
            .get(0);

    assertThat(sc.getConfig().getS3AccessKeyId()).isEqualTo("AKIA123");
    assertThat(sc.getConfig().getS3SecretAccessKey()).isEqualTo("secret");
    assertThat(sc.getConfig().getS3SessionToken()).isNull();
  }

  @Test
  public void testNullProviderCredentials() {
    // No provider creds at all (shouldn't happen in prod but guards against NPE)
    TemporaryCredentials uc = new TemporaryCredentials().expirationTime(1700000000000L);

    StorageCredential sc =
        DeltaCredentialsMapper.toCredentialsResponse(
                "s3://bucket/path", uc, CredentialOperation.READ)
            .getStorageCredentials()
            .get(0);

    StorageCredentialConfig config = sc.getConfig();
    assertThat(config.getS3AccessKeyId()).isNull();
    assertThat(config.getS3SecretAccessKey()).isNull();
    assertThat(config.getS3SessionToken()).isNull();
    assertThat(config.getAzureSasToken()).isNull();
    assertThat(config.getGcsOauthToken()).isNull();
    assertThat(sc.getExpirationTimeMs()).isEqualTo(1700000000000L);
  }

  /**
   * Pins the sparse-JSON wire contract. StorageCredentialConfig is a typed POJO whose unset fields
   * are null in Java. The Delta REST Catalog ObjectMapper is configured with {@link
   * JsonInclude.Include#NON_NULL} in UnityCatalogServer so the response omits keys for clouds that
   * don't apply. If that mapper config is ever changed (or the generated class is regenerated with
   * a default {@code USE_DEFAULTS} policy), this test fails loudly.
   */
  @Test
  public void testSparseJsonOmitsUnpopulatedKeys() throws Exception {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA123")
                    .secretAccessKey("secret")
                    .sessionToken("token"))
            .expirationTime(1700000000000L);
    StorageCredentialConfig config =
        DeltaCredentialsMapper.toCredentialsResponse(
                "s3://bucket/path", uc, CredentialOperation.READ)
            .getStorageCredentials()
            .get(0)
            .getConfig();

    // Use the same mapper config as the Delta REST Catalog response converter.
    ObjectMapper deltaMapper =
        JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build();
    String json = deltaMapper.writeValueAsString(config);

    assertThat(json)
        .contains("\"s3.access-key-id\":\"AKIA123\"")
        .contains("\"s3.secret-access-key\":\"secret\"")
        .contains("\"s3.session-token\":\"token\"")
        .doesNotContain("azure.sas-token")
        .doesNotContain("gcs.oauth-token");
  }
}
