package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.unitycatalog.server.delta.model.DeltaCredentialOperation;
import io.unitycatalog.server.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.server.delta.model.DeltaStorageCredential;
import io.unitycatalog.server.delta.model.DeltaStorageCredentialConfig;
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
            .url("s3://bucket/path")
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA123")
                    .secretAccessKey("secret")
                    .sessionToken("token"))
            .expirationTime(1700000000000L);

    DeltaCredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ);

    assertThat(resp.getStorageCredentials()).hasSize(1);
    DeltaStorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getPrefix()).isEqualTo("s3://bucket/path");
    assertThat(sc.getOperation()).isEqualTo(DeltaCredentialOperation.READ);
    assertThat(sc.getExpirationTimeMs()).isEqualTo(1700000000000L);
    DeltaStorageCredentialConfig config = sc.getConfig();
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
            .url("abfss://container@acct.dfs.core.windows.net/path")
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sv=..."))
            .expirationTime(1700000000000L);

    DeltaCredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ_WRITE);

    DeltaStorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getOperation()).isEqualTo(DeltaCredentialOperation.READ_WRITE);
    assertThat(sc.getConfig().getAzureSasToken()).isEqualTo("sv=...");
    assertThat(sc.getConfig().getS3AccessKeyId()).isNull();
  }

  @Test
  public void testGcpCredentials() {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .url("gs://bucket/path")
            .gcpOauthToken(new GcpOauthToken().oauthToken("ya29..."))
            .expirationTime(1700000000000L);

    DeltaCredentialsResponse resp =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ);

    DeltaStorageCredential sc = resp.getStorageCredentials().get(0);
    assertThat(sc.getConfig().getGcsOauthToken()).isEqualTo("ya29...");
    assertThat(sc.getConfig().getS3AccessKeyId()).isNull();
  }

  @Test
  public void testPartialAwsCredentials() {
    // Only access key + secret, no session token (permanent credentials case)
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .url("s3://bucket/path")
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("AKIA123").secretAccessKey("secret"))
            .expirationTime(1700000000000L);

    DeltaStorageCredential sc =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ)
            .getStorageCredentials()
            .get(0);

    assertThat(sc.getConfig().getS3AccessKeyId()).isEqualTo("AKIA123");
    assertThat(sc.getConfig().getS3SecretAccessKey()).isEqualTo("secret");
    assertThat(sc.getConfig().getS3SessionToken()).isNull();
  }

  @Test
  public void testNullProviderCredentials() {
    // No provider creds or url at all (shouldn't happen in prod but guards against NPE)
    TemporaryCredentials uc = new TemporaryCredentials().expirationTime(1700000000000L);

    DeltaStorageCredential sc =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ)
            .getStorageCredentials()
            .get(0);

    // A null url maps straight through to a null prefix.
    assertThat(sc.getPrefix()).isNull();
    DeltaStorageCredentialConfig config = sc.getConfig();
    assertThat(config.getS3AccessKeyId()).isNull();
    assertThat(config.getS3SecretAccessKey()).isNull();
    assertThat(config.getS3SessionToken()).isNull();
    assertThat(config.getAzureSasToken()).isNull();
    assertThat(config.getGcsOauthToken()).isNull();
    assertThat(sc.getExpirationTimeMs()).isEqualTo(1700000000000L);
  }

  /**
   * Pins the sparse-JSON wire contract. DeltaStorageCredentialConfig is a typed POJO whose unset
   * fields are null in Java. The UC Delta API ObjectMapper is configured with {@link
   * JsonInclude.Include#NON_NULL} in {@link DeltaApiMappers} so the response omits keys for clouds
   * that don't apply. If that mapper config is ever changed (or the generated class is regenerated
   * with a default {@code USE_DEFAULTS} policy), this test fails loudly.
   */
  @Test
  public void testSparseJsonOmitsUnpopulatedKeys() throws Exception {
    TemporaryCredentials uc =
        new TemporaryCredentials()
            .url("s3://bucket/path")
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA123")
                    .secretAccessKey("secret")
                    .sessionToken("token"))
            .expirationTime(1700000000000L);
    DeltaStorageCredentialConfig config =
        DeltaCredentialsMapper.toCredentialsResponse(uc, DeltaCredentialOperation.READ)
            .getStorageCredentials()
            .get(0)
            .getConfig();

    // Use the same mapper as the UC Delta API response converter, so a change to the
    // wire-format config (e.g. inclusion policy) is caught here instead of drifting silently.
    String json = DeltaApiMappers.MAPPER.writeValueAsString(config);

    assertThat(json)
        .contains("\"s3.access-key-id\":\"AKIA123\"")
        .contains("\"s3.secret-access-key\":\"secret\"")
        .contains("\"s3.session-token\":\"token\"")
        .doesNotContain("azure.sas-token")
        .doesNotContain("gcs.oauth-token");
  }
}
