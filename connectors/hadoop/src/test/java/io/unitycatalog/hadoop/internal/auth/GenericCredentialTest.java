package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GenericCredentialTest {

  @Test
  void awsCredentialExposesItsFields() {
    AwsCredential aws = new AwsCredential("ak", "sk", "st", 123L);
    assertThat(aws.accessKeyId()).isEqualTo("ak");
    assertThat(aws.secretAccessKey()).isEqualTo("sk");
    assertThat(aws.sessionToken()).isEqualTo("st");
    assertThat(aws.expirationTimeMillis()).isEqualTo(123L);
  }

  @Test
  void azureCredentialExposesItsFields() {
    AzureCredential azure = new AzureCredential("sas", 456L);
    assertThat(azure.sasToken()).isEqualTo("sas");
    assertThat(azure.expirationTimeMillis()).isEqualTo(456L);
  }

  @Test
  void gcsCredentialExposesItsFieldsAndAllowsNullExpiration() {
    GcsCredential gcs = new GcsCredential("oauth", null);
    assertThat(gcs.oauthToken()).isEqualTo("oauth");
    assertThat(gcs.expirationTimeMillis()).isNull();
  }

  @Test
  void equalsAndHashCodeUseAllFields() {
    AwsCredential cred = new AwsCredential("ak", "sk", "st", 1L);
    AwsCredential same = new AwsCredential("ak", "sk", "st", 1L);
    AwsCredential differentToken = new AwsCredential("ak", "sk", "st2", 1L);
    AwsCredential differentExpiry = new AwsCredential("ak", "sk", "st", 2L);

    assertThat(cred).isEqualTo(same).hasSameHashCodeAs(same);
    assertThat(cred).isNotEqualTo(differentToken).isNotEqualTo(differentExpiry);
    assertThat((GenericCredential) cred).isNotEqualTo(new GcsCredential("ak", 1L));
  }
}
