package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
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
    DeltaStorageCredential only = credAt("s3://other-bucket");
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
    DeltaStorageCredential bucket = credAt("s3://bucket");
    DeltaStorageCredential table = credAt("s3://bucket/t");
    DeltaStorageCredential child = credAt("s3://bucket/t/child");
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
    List<DeltaStorageCredential> creds =
        Arrays.asList(null, new DeltaStorageCredential(), credAt("s3://bucket/t"));
    assertThat(DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds).getPrefix())
        .isEqualTo("s3://bucket/t");
  }

  @Test
  void selectorThrowsWhenMultiResponseHasNoMatch() {
    List<DeltaStorageCredential> creds =
        Arrays.asList(credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void toGenericCredentialExtractsAwsKeysAndExpiry() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(123L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    AwsCredential gc = (AwsCredential) DeltaStorageCredentialUtil.toGenericCredential(c);
    assertThat(gc.expirationTimeMillis()).isEqualTo(123L);
    assertThat(gc.accessKeyId()).isEqualTo("ak");
    assertThat(gc.secretAccessKey()).isEqualTo("sk");
    assertThat(gc.sessionToken()).isEqualTo("st");
  }

  @Test
  void toGenericCredentialRejectsMultiCloudConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3AccessKeyId("ak").gcsOauthToken("gcs"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toGenericCredential(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain exactly one cloud credential config");
  }

  @Test
  void toGenericCredentialRejectsMissingConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential().prefix("s3://bucket").operation(DeltaCredentialOperation.READ);
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toGenericCredential(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing config");
  }

  @Test
  void toGenericCredentialExtractsAzureSasToken() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("abfss://container@account.dfs.core.windows.net/")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .config(new DeltaStorageCredentialConfig().azureSasToken("sas-token"));
    AzureCredential gc = (AzureCredential) DeltaStorageCredentialUtil.toGenericCredential(c);
    assertThat(gc.sasToken()).isEqualTo("sas-token");
    assertThat(gc.expirationTimeMillis()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toGenericCredentialExtractsGcsOauthToken() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("gs://bucket/")
            .operation(DeltaCredentialOperation.READ)
            .expirationTimeMs(456L)
            .config(new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth-token"));
    GcsCredential gc = (GcsCredential) DeltaStorageCredentialUtil.toGenericCredential(c);
    assertThat(gc.oauthToken()).isEqualTo("gcs-oauth-token");
    assertThat(gc.expirationTimeMillis()).isEqualTo(456L);
  }

  @Test
  void toGenericCredentialRejectsPartialS3WithMissingAccessKey() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3SessionToken("st"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toGenericCredential(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing S3 access key");
  }

  private static DeltaStorageCredential credAt(String prefix) {
    return new DeltaStorageCredential().prefix(prefix);
  }
}
