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

  @Test
  void credPrefixesRoundTripInOrder() {
    List<String> prefixes =
        List.of(
            "s3://bucket/table",
            "s3://bucket/table,archive",
            "s3://bucket/table/%20/${credential}",
            "s3://bucket/table/%2C/%25/%2F",
            "s3://bucket/table with spaces",
            "s3://bucket/table\twith\ttabs",
            "s3://bucket/table-name_with.parts~",
            "s3://bucket/table?key=value&other=a+b#fragment",
            "s3://bucket/café");

    String[] encodedPrefixes = CredentialUtil.encodeCredPrefixes(prefixes);

    assertThat(encodedPrefixes).doesNotContainAnyElementsOf(prefixes);
    assertThat(encodedPrefixes).allMatch(prefix -> !prefix.contains(","));
    assertThat(CredentialUtil.decodeCredPrefixes(encodedPrefixes))
        .containsExactlyElementsOf(prefixes);
  }

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
                .expirationTime(EXPIRATION)
                .url("s3://bucket/table"),
            new AwsCredential(
                "access-key", "secret-key", "session-token", EXPIRATION, "s3://bucket/table")),
        Arguments.of(
            "Azure",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token"))
                .expirationTime(EXPIRATION)
                .url("abfss://container@account/table"),
            new AzureCredential("sas-token", EXPIRATION, "abfss://container@account/table")),
        Arguments.of(
            "GCS",
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("oauth-token"))
                .expirationTime(EXPIRATION)
                .url("gs://bucket/table"),
            new GcsCredential("oauth-token", EXPIRATION, "gs://bucket/table")));
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
        .hasMessageContaining("requires multiple storage credentials");
    assertThatThrownBy(
            () -> CredentialUtil.selectForLocation("s3://bucket/t", Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires multiple storage credentials");
  }

  @Test
  void selectorRequiresMultipleCreds() {
    assertThatThrownBy(
            () -> CredentialUtil.selectForLocation("s3://bucket/t", List.of(credAt("s3://bucket"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires multiple storage credentials");
    assertThatThrownBy(
            () ->
                CredentialUtil.selectForLocation("s3://bucket/t", Collections.singletonList(null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires multiple storage credentials");
  }

  @Test
  void selectorPicksLongestMatchingPrefix() {
    GenericCredential bucket = credAt("s3://bucket");
    GenericCredential table = credAt("s3://bucket/t");
    GenericCredential child = credAt("s3://bucket/t/child");
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
  void selectorIgnoresNullAndPrefixlessInMultiResponse() {
    List<GenericCredential> creds = Arrays.asList(null, credAt(null), credAt("s3://bucket/t"));
    assertThat(CredentialUtil.selectForLocation("s3://bucket/t", creds).prefix())
        .isEqualTo("s3://bucket/t");
  }

  @Test
  void selectorThrowsWhenMultiResponseHasNoMatch() {
    List<GenericCredential> creds =
        Arrays.asList(credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> CredentialUtil.selectForLocation("s3://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential covers location");
    // Scheme aliases are not normalized; otherwise, the s3 prefix would cover the s3a location.
    assertThatThrownBy(
            () ->
                CredentialUtil.selectForLocation(
                    "s3a://bucket/t",
                    Arrays.asList(credAt("s3://bucket"), credAt("s3://bucket/t"))))
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
    assertThat(gc.prefix()).isEqualTo("s3://bucket");
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
    assertThat(gc.prefix()).isEqualTo("abfss://container@account.dfs.core.windows.net/");
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
    assertThat(gc.prefix()).isEqualTo("gs://bucket/");
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

  @ParameterizedTest
  @MethodSource("coveringPrefixCases")
  void longestCoveringIndexMatchesLocationToPrefix(
      List<String> prefixes, String location, int expectedIndex) {
    assertThat(CredentialUtil.longestCoveringIndex(location, prefixes)).isEqualTo(expectedIndex);
  }

  private static Stream<Arguments> coveringPrefixCases() {
    List<String> nested =
        Arrays.asList("s3://bucket/table", "s3://bucket/table/child", "s3://bucket");
    return Stream.of(
        // Longest (most specific) covering prefix wins over shorter ancestors.
        Arguments.of(nested, "s3://bucket/table/child/data", 1),
        // A location under only the broader prefix selects it, not the deeper sibling.
        Arguments.of(nested, "s3://bucket/table/x", 0),
        // Trailing slashes on the prefix are normalized away.
        Arguments.of(List.of("s3://bucket/t///"), "s3://bucket/t/data", 0),
        // Null and empty prefixes are skipped; the covering one is chosen.
        Arguments.of(Arrays.asList(null, "", "s3://bucket/t"), "s3://bucket/t/data", 2),
        // On equal-length normalized prefixes, the first covering one wins.
        Arguments.of(
            Arrays.asList("s3://bucket/table/", "s3://bucket/table"), "s3://bucket/table/data", 0));
  }

  @ParameterizedTest
  @MethodSource("nonCoveringPrefixCases")
  void longestCoveringIndexReturnsNegativeOneWhenNoPrefixMatches(
      List<String> prefixes, String location) {
    assertThat(CredentialUtil.longestCoveringIndex(location, prefixes)).isEqualTo(-1);
  }

  private static Stream<Arguments> nonCoveringPrefixCases() {
    return Stream.of(
        Arguments.of(List.of(), "s3://bucket/table"),
        Arguments.of(Arrays.asList(null, ""), "s3://bucket/table"),
        // Scheme aliases are compared literally.
        Arguments.of(List.of("s3://bucket/table"), "s3a://bucket/table/data"),
        Arguments.of(List.of("abfs://c@a/t"), "abfss://c@a/t"),
        // Scheme comparison is case-sensitive.
        Arguments.of(List.of("s3://bucket/t"), "S3://bucket/t"),
        // Different clouds never match, even with the same bucket/path.
        Arguments.of(List.of("s3://bucket/t"), "gs://bucket/t"),
        // Unknown schemes are compared literally (case-sensitive), so a case mismatch fails.
        Arguments.of(List.of("hdfs://nn/t"), "HDFS://nn/t"),
        // No prefix covers the location.
        Arguments.of(Arrays.asList("s3://other", "s3://bucket/sibling"), "s3://bucket/t"));
  }

  @Test
  void longestCoveringIndexRejectsNullPrefixList() {
    assertThatThrownBy(() -> CredentialUtil.longestCoveringIndex("s3://bucket/table", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("List of prefixes cannot be null.");
  }

  private static GenericCredential credAt(String location) {
    return new AwsCredential("ak", "sk", "st", 1L, location);
  }
}
