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
  void selectForLocationPicksLongestCoveringPrefix() {
    GenericCredential unscoped = credAt(null);
    GenericCredential bucket = credAt("s3://bucket");
    GenericCredential table = credAt("s3://bucket/t");
    GenericCredential child = credAt("s3://bucket/t/child");
    assertThat(
            CredentialUtil.selectForLocation(
                "s3://bucket/t/child/file", Arrays.asList(unscoped, bucket, table, child)))
        .isSameAs(child);
  }

  @Test
  void selectForLocationMatchesAcrossSchemeAliases() {
    GenericCredential s3 = credAt("s3://bucket/t");
    GenericCredential abfs = credAt("abfs://c@a/t");

    assertThat(CredentialUtil.selectForLocation("s3a://bucket/t/file", List.of(s3))).isSameAs(s3);
    assertThat(CredentialUtil.selectForLocation("S3A://bucket/t/file", List.of(s3))).isSameAs(s3);
    assertThat(CredentialUtil.selectForLocation("abfss://c@a/t/file", List.of(abfs)))
        .isSameAs(abfs);
    assertThat(CredentialUtil.selectForLocation("ABFSS://c@a/t/file", List.of(abfs)))
        .isSameAs(abfs);
  }

  @Test
  void selectForLocationThrowsWhenNoPrefixCovers() {
    List<GenericCredential> creds =
        Arrays.asList(credAt(null), credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("s3://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential covers location");
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("S3A://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential covers location");
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("longestCoveringIndexCases")
  void longestCoveringIndexMatchesLocationToPrefix(
      String description, List<String> prefixes, String location, int expectedIndex) {
    assertThat(CredentialUtil.longestCoveringIndex(location, prefixes)).isEqualTo(expectedIndex);
  }

  private static Stream<Arguments> longestCoveringIndexCases() {
    List<String> nested =
        Arrays.asList("s3://bucket/table", "s3://bucket/table/clone", "s3://bucket");
    return Stream.of(
        // Longest (most specific) covering prefix wins over shorter ancestors.
        Arguments.of("most specific prefix wins", nested, "s3://bucket/table/clone/data", 1),
        // A location under only the broader prefix selects it, not the deeper sibling.
        Arguments.of("shorter prefix when deeper does not cover", nested, "s3://bucket/table/x", 0),
        // Scheme aliases are normalized: s3a location matches an s3 prefix.
        Arguments.of(
            "s3a location matches s3 prefix",
            List.of("s3://bucket/table"),
            "s3a://bucket/table/data",
            0),
        // abfss/abfs aliases are normalized.
        Arguments.of(
            "abfss location matches abfs prefix", List.of("abfs://c@a/t"), "abfss://c@a/t/data", 0),
        // Scheme case is normalized.
        Arguments.of("uppercase scheme matches", List.of("s3://bucket/t"), "S3://bucket/t/data", 0),
        // Trailing slashes on the prefix are normalized away.
        Arguments.of(
            "trailing slashes ignored", List.of("s3://bucket/t///"), "s3://bucket/t/data", 0),
        // Different clouds never match, even with the same bucket/path.
        Arguments.of(
            "cross-cloud does not match", List.of("s3://bucket/t"), "gs://bucket/t/data", -1),
        // Unknown schemes are compared literally (case-sensitive), so a case mismatch fails.
        Arguments.of("unknown scheme is case-sensitive", List.of("hdfs://nn/t"), "HDFS://nn/t", -1),
        // Null and empty prefixes are skipped; the covering one is chosen.
        Arguments.of(
            "null and empty prefixes skipped",
            Arrays.asList(null, "", "s3://bucket/t"),
            "s3://bucket/t/data",
            2),
        // No prefix covers the location.
        Arguments.of(
            "no covering prefix",
            Arrays.asList("s3://other", "s3://bucket/sibling"),
            "s3://bucket/t",
            -1),
        // On equal-length canonical prefixes, the first covering one wins.
        Arguments.of(
            "first of equal-length prefixes wins",
            Arrays.asList("s3://bucket/table/", "s3://bucket/table"),
            "s3://bucket/table/data",
            0));
  }

  private static GenericCredential credAt(String location) {
    return new AwsCredential("ak", "sk", "st", 1L, location);
  }
}
