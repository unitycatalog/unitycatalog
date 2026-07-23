package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CredentialUtilTest {
  private static final long EXPIRATION = 123L;

  @ParameterizedTest(name = "{0}")
  @MethodSource("validCredentials")
  void convertsTemporaryCredentials(
      String cloud, TemporaryCredentials input, GenericCredential expected) {
    assertThat(CredentialUtil.toGenericCredential(input)).isEqualTo(expected);
  }

  private static Stream<Arguments> validCredentials() {
    return Stream.of(
        Arguments.of(
            "AWS",
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("access-key")
                        .secretAccessKey("secret-key")
                        .sessionToken("session-token"))
                .expirationTime(EXPIRATION),
            new AwsCredential("access-key", "secret-key", "session-token", EXPIRATION, null)),
        Arguments.of(
            "Azure",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token"))
                .expirationTime(EXPIRATION),
            new AzureCredential("sas-token", EXPIRATION, null)),
        Arguments.of(
            "GCS",
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("oauth-token"))
                .expirationTime(EXPIRATION),
            new GcsCredential("oauth-token", EXPIRATION, null)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidCredentials")
  void rejectsMissingRequiredField(
      String cloud, TemporaryCredentials input, String expectedMessage) {
    assertThatThrownBy(() -> CredentialUtil.toGenericCredential(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  private static Stream<Arguments> invalidCredentials() {
    return Stream.of(
        Arguments.of(
            "AWS",
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .secretAccessKey("secret-key")
                        .sessionToken("session-token")),
            "AWS access key is missing"),
        Arguments.of(
            "Azure",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("")),
            "Azure SAS token is missing"),
        Arguments.of(
            "GCS",
            new TemporaryCredentials().gcpOauthToken(new GcpOauthToken()),
            "GCS OAuth token is missing"),
        Arguments.of(
            "no credential",
            new TemporaryCredentials(),
            "UC temporary credentials contained no cloud credential"));
  }

  @Test
  void selectorRejectsMissingResponse() {
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("s3://bucket/t", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
    assertThatThrownBy(
            () -> CredentialUtil.selectForLocation("s3://bucket/t", Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
  }

  @Test
  void selectorRejectsSingleWithoutPrefixMatch() {
    DeltaStorageCredential only = credAt("s3://other-bucket");
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("s3://bucket/t", List.of(only)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void selectorRejectsSingleNull() {
    assertThatThrownBy(
            () ->
                CredentialUtil.selectForLocation("s3://bucket/t", Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("contains null");
  }

  @Test
  void selectorPicksLongestMatchingPrefix() {
    DeltaStorageCredential bucket = credAt("s3://bucket");
    DeltaStorageCredential table = credAt("s3://bucket/t");
    DeltaStorageCredential child = credAt("s3://bucket/t/child");
    assertThat(
            CredentialUtil.selectForLocation(
                "s3://bucket/t/child/file", Arrays.asList(bucket, table, child)))
        .isSameAs(child);
  }

  @Test
  void selectorMatchesAtPathBoundary() {
    assertThat(CredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("s3://bucket/t/x", "s3://bucket/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("s3://bucket/t-other", "s3://bucket/t")).isFalse();
  }

  @Test
  void selectorNormalizesTrailingSlashes() {
    assertThat(CredentialUtil.prefixCovers("s3://bucket/t//", "s3://bucket/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t///")).isTrue();
  }

  @Test
  void selectorMatchesAcrossSchemeAliases() {
    DeltaStorageCredential s3Prefix = credAt("s3://bucket/t");
    assertThat(
            CredentialUtil.selectForLocation("s3a://bucket/t/file", List.of(s3Prefix)))
        .isSameAs(s3Prefix);
  }

  @Test
  void prefixCoversNormalizesSchemeAliasesAndCase() {
    assertThat(CredentialUtil.prefixCovers("s3a://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("S3://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("abfss://c@a/t", "abfs://c@a/t")).isTrue();
  }

  @Test
  void prefixCoversDoesNotMatchAcrossClouds() {
    assertThat(CredentialUtil.prefixCovers("gs://bucket/t", "s3://bucket/t")).isFalse();
  }

  @Test
  void prefixCoversLeavesUnknownSchemesUnnormalized() {
    assertThat(CredentialUtil.prefixCovers("hdfs://nn/t", "hdfs://nn/t")).isTrue();
    assertThat(CredentialUtil.prefixCovers("HDFS://nn/t", "hdfs://nn/t")).isFalse();
  }

  @Test
  void selectorIgnoresNullAndPrefixlessInMultiResponse() {
    List<DeltaStorageCredential> creds =
        Arrays.asList(null, new DeltaStorageCredential(), credAt("s3://bucket/t"));
    assertThat(CredentialUtil.selectForLocation("s3://bucket/t", creds).getPrefix())
        .isEqualTo("s3://bucket/t");
  }

  @Test
  void selectorThrowsWhenMultiResponseHasNoMatch() {
    List<DeltaStorageCredential> creds =
        Arrays.asList(credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("s3://bucket/t", creds))
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
    AwsCredential gc = (AwsCredential) CredentialUtil.toGenericCredential(c);
    assertThat(gc.expirationTimeMillis()).isEqualTo(123L);
    assertThat(gc.accessKeyId()).isEqualTo("ak");
    assertThat(gc.secretAccessKey()).isEqualTo("sk");
    assertThat(gc.sessionToken()).isEqualTo("st");
    assertThat(gc.location()).isEqualTo("s3://bucket");
  }

  @Test
  void toGenericCredentialRejectsMultiCloudConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3AccessKeyId("ak").gcsOauthToken("gcs"));
    assertThatThrownBy(() -> CredentialUtil.toGenericCredential(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain exactly one cloud credential config");
  }

  @Test
  void toGenericCredentialRejectsMissingConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential().prefix("s3://bucket").operation(DeltaCredentialOperation.READ);
    assertThatThrownBy(() -> CredentialUtil.toGenericCredential(c))
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
    AzureCredential gc = (AzureCredential) CredentialUtil.toGenericCredential(c);
    assertThat(gc.sasToken()).isEqualTo("sas-token");
    assertThat(gc.expirationTimeMillis()).isEqualTo(Long.MAX_VALUE);
    assertThat(gc.location()).isEqualTo("abfss://container@account.dfs.core.windows.net/");
  }

  @Test
  void toGenericCredentialExtractsGcsOauthToken() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("gs://bucket/")
            .operation(DeltaCredentialOperation.READ)
            .expirationTimeMs(456L)
            .config(new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth-token"));
    GcsCredential gc = (GcsCredential) CredentialUtil.toGenericCredential(c);
    assertThat(gc.oauthToken()).isEqualTo("gcs-oauth-token");
    assertThat(gc.expirationTimeMillis()).isEqualTo(456L);
    assertThat(gc.location()).isEqualTo("gs://bucket/");
  }

  @Test
  void toGenericCredentialRejectsPartialS3WithMissingAccessKey() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3SessionToken("st"));
    assertThatThrownBy(() -> CredentialUtil.toGenericCredential(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("AWS access key is missing");
  }

  private static DeltaStorageCredential credAt(String prefix) {
    return new DeltaStorageCredential().prefix(prefix);
  }
}
