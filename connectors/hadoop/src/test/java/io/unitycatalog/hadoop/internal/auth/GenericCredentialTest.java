package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GenericCredentialTest {
  @ParameterizedTest(name = "{0}")
  @MethodSource("equalCredentials")
  void equalCredentialsHaveSameHashCode(
      String description, GenericCredential left, GenericCredential right) {
    assertThat(left).isEqualTo(right);
    assertThat(left.hashCode()).isEqualTo(right.hashCode());
  }

  private static Stream<Arguments> equalCredentials() {
    return Stream.of(
        Arguments.of(
            "AWS credentials",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t")),
        Arguments.of(
            "Azure credentials",
            new AzureCredential("sas", 1L, "abfs://container/t"),
            new AzureCredential("sas", 1L, "abfs://container/t")),
        Arguments.of(
            "GCS credentials with null expiration",
            new GcsCredential("oauth", null, "gs://bucket/t"),
            new GcsCredential("oauth", null, "gs://bucket/t")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unequalCredentials")
  void unequalCredentialsHaveDifferentHashCodes(
      String description, GenericCredential left, GenericCredential right) {
    assertThat(left).isNotEqualTo(right);
    assertThat(right).isNotEqualTo(left);
    assertThat(left.hashCode()).isNotEqualTo(right.hashCode());
  }

  private static Stream<Arguments> unequalCredentials() {
    return Stream.of(
        Arguments.of(
            "different AWS access key",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak2", "sk", "st", 1L, "s3://bucket/t")),
        Arguments.of(
            "different AWS secret key",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak", "sk2", "st", 1L, "s3://bucket/t")),
        Arguments.of(
            "different AWS session token",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak", "sk", "st2", 1L, "s3://bucket/t")),
        Arguments.of(
            "different AWS expiration",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak", "sk", "st", 2L, "s3://bucket/t")),
        Arguments.of(
            "different AWS location",
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/t"),
            new AwsCredential("ak", "sk", "st", 1L, "s3://bucket/other")),
        Arguments.of(
            "different Azure SAS token",
            new AzureCredential("sas", 1L, "abfs://container/t"),
            new AzureCredential("sas2", 1L, "abfs://container/t")),
        Arguments.of(
            "different Azure expiration",
            new AzureCredential("sas", 1L, "abfs://container/t"),
            new AzureCredential("sas", 2L, "abfs://container/t")),
        Arguments.of(
            "different GCS OAuth token",
            new GcsCredential("oauth", 1L, "gs://bucket/t"),
            new GcsCredential("oauth2", 1L, "gs://bucket/t")),
        Arguments.of(
            "different GCS expiration",
            new GcsCredential("oauth", 1L, "gs://bucket/t"),
            new GcsCredential("oauth", null, "gs://bucket/t")),
        Arguments.of(
            "different credential subtypes",
            new AzureCredential("azure", 1L, null),
            new GcsCredential("gcs", 1L, null)));
  }

  @Test
  void cloudCredentialsExposeTheirFields() {
    AwsCredential aws = new AwsCredential("ak", "sk", "st", 123L, "s3://bucket/t");
    assertThat(aws.accessKeyId()).isEqualTo("ak");
    assertThat(aws.secretAccessKey()).isEqualTo("sk");
    assertThat(aws.sessionToken()).isEqualTo("st");
    assertThat(aws.expirationTimeMillis()).isEqualTo(123L);
    assertThat(aws.location()).isEqualTo("s3://bucket/t");

    AzureCredential azure = new AzureCredential("sas", 456L, null);
    assertThat(azure.sasToken()).isEqualTo("sas");
    assertThat(azure.expirationTimeMillis()).isEqualTo(456L);
    assertThat(azure.location()).isNull();

    GcsCredential gcs = new GcsCredential("oauth", null, "gs://bucket/t");
    assertThat(gcs.oauthToken()).isEqualTo("oauth");
    assertThat(gcs.expirationTimeMillis()).isNull();
    assertThat(gcs.location()).isEqualTo("gs://bucket/t");
  }
}
