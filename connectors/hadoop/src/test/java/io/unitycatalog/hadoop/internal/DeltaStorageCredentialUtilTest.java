package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeltaStorageCredentialUtilTest {

  @Test
  void selectorRejectsMissingResponse() {
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
    assertThatThrownBy(
            () ->
                DeltaStorageCredentialUtil.selectForLocation(
                    "s3://bucket/t", Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
  }

  @Test
  void selectorRejectsSingleWithoutPrefixMatch() {
    StorageCredential only = credAt("s3://other-bucket");
    assertThatThrownBy(
            () -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", List.of(only)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void selectorRejectsSingleNull() {
    assertThatThrownBy(
            () ->
                DeltaStorageCredentialUtil.selectForLocation(
                    "s3://bucket/t", Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("contains null");
  }

  @Test
  void selectorPicksLongestMatchingPrefix() {
    StorageCredential bucket = credAt("s3://bucket");
    StorageCredential table = credAt("s3://bucket/t");
    StorageCredential child = credAt("s3://bucket/t/child");
    assertThat(
            DeltaStorageCredentialUtil.selectForLocation(
                "s3://bucket/t/child/file", Arrays.asList(bucket, table, child)))
        .isSameAs(child);
  }

  @Test
  void selectorMatchesAtPathBoundary() {
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t/x", "s3://bucket/t"))
        .isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t-other", "s3://bucket/t"))
        .isFalse();
  }

  @Test
  void selectorNormalizesTrailingSlashes() {
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t//", "s3://bucket/t"))
        .isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t///"))
        .isTrue();
  }

  @Test
  void selectorIgnoresNullAndPrefixlessInMultiResponse() {
    List<StorageCredential> creds =
        Arrays.asList(null, new StorageCredential(), credAt("s3://bucket/t"));
    assertThat(DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds).getPrefix())
        .isEqualTo("s3://bucket/t");
  }

  @Test
  void selectorThrowsWhenMultiResponseHasNoMatch() {
    List<StorageCredential> creds =
        Arrays.asList(credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void toTemporaryCredentialsExtractsAwsKeysAndExpiry() {
    StorageCredential c =
        new StorageCredential()
            .prefix("s3://bucket")
            .operation(CredentialOperation.READ_WRITE)
            .expirationTimeMs(123L)
            .config(
                new StorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getExpirationTime()).isEqualTo(123L);
    assertThat(tc.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(tc.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(tc.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
  }

  @Test
  void toTemporaryCredentialsRejectsMultiCloudConfig() {
    StorageCredential c =
        new StorageCredential()
            .prefix("s3://bucket")
            .operation(CredentialOperation.READ)
            .config(new StorageCredentialConfig().s3AccessKeyId("ak").gcsOauthToken("gcs"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain exactly one cloud credential config");
  }

  @Test
  void toTemporaryCredentialsRejectsMissingConfig() {
    StorageCredential c =
        new StorageCredential().prefix("s3://bucket").operation(CredentialOperation.READ);
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing config");
  }

  @Test
  void toTemporaryCredentialsExtractsAzureSasToken() {
    StorageCredential c =
        new StorageCredential()
            .prefix("abfss://container@account.dfs.core.windows.net/")
            .operation(CredentialOperation.READ_WRITE)
            .config(new StorageCredentialConfig().azureSasToken("sas-token"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getAzureUserDelegationSas().getSasToken()).isEqualTo("sas-token");
    assertThat(tc.getExpirationTime()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toTemporaryCredentialsExtractsGcsOauthToken() {
    StorageCredential c =
        new StorageCredential()
            .prefix("gs://bucket/")
            .operation(CredentialOperation.READ)
            .expirationTimeMs(456L)
            .config(new StorageCredentialConfig().gcsOauthToken("gcs-oauth-token"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getGcpOauthToken().getOauthToken()).isEqualTo("gcs-oauth-token");
    assertThat(tc.getExpirationTime()).isEqualTo(456L);
  }

  @Test
  void toTemporaryCredentialsRejectsPartialS3WithMissingAccessKey() {
    StorageCredential c =
        new StorageCredential()
            .prefix("s3://bucket")
            .operation(CredentialOperation.READ)
            .config(new StorageCredentialConfig().s3SessionToken("st"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing S3 access key");
  }

  private static StorageCredential credAt(String prefix) {
    return new StorageCredential().prefix(prefix);
  }
}
