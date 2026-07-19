package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class GenericCredentialTest {

  @Test
  void factoriesSetExactlyTheirOwnFamily() {
    GenericCredential aws = GenericCredential.forAws("ak", "sk", "st", 123L);
    assertThat(aws.awsAccessKeyId()).isEqualTo("ak");
    assertThat(aws.awsSecretAccessKey()).isEqualTo("sk");
    assertThat(aws.awsSessionToken()).isEqualTo("st");
    assertThat(aws.azureSasToken()).isNull();
    assertThat(aws.gcsOauthToken()).isNull();
    assertThat(aws.expirationTimeMillis()).isEqualTo(123L);

    GenericCredential azure = GenericCredential.forAzure("sas", 456L);
    assertThat(azure.azureSasToken()).isEqualTo("sas");
    assertThat(azure.awsAccessKeyId()).isNull();
    assertThat(azure.gcsOauthToken()).isNull();
    assertThat(azure.expirationTimeMillis()).isEqualTo(456L);

    // Also covers the nullable-expiration.
    GenericCredential gcs = GenericCredential.forGcs("oauth", null);
    assertThat(gcs.gcsOauthToken()).isEqualTo("oauth");
    assertThat(gcs.awsAccessKeyId()).isNull();
    assertThat(gcs.azureSasToken()).isNull();
    assertThat(gcs.expirationTimeMillis()).isNull();
  }

  @Test
  void rejectsCredentialWithNoCloudFamily() {
    assertThatThrownBy(() -> GenericCredential.forGcs(null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly one cloud family");
    assertThatThrownBy(() -> GenericCredential.forAzure(null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly one cloud family");
    assertThatThrownBy(() -> GenericCredential.forAws(null, null, null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly one cloud family");
  }

  @Test
  void equalsAndHashCodeUseAllFields() {
    GenericCredential cred = GenericCredential.forAws("ak", "sk", "st", 1L);
    GenericCredential same = GenericCredential.forAws("ak", "sk", "st", 1L);
    GenericCredential differentToken = GenericCredential.forAws("ak", "sk", "st2", 1L);
    GenericCredential differentExpiry = GenericCredential.forAws("ak", "sk", "st", 2L);

    assertThat(cred).isEqualTo(same).hasSameHashCodeAs(same);
    assertThat(cred).isNotEqualTo(differentToken).isNotEqualTo(differentExpiry);
    assertThat(cred).isNotEqualTo(GenericCredential.forGcs("ak", 1L));
  }
}
