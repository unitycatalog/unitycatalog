package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import java.util.List;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

final class UCDeltaCredentialFetcherTestUtils {
  private UCDeltaCredentialFetcherTestUtils() {}

  static DeltaCredentialsResponse s3Response(String... prefixes) {
    DeltaCredentialsResponse response = new DeltaCredentialsResponse();
    for (int i = 0; i < prefixes.length; i++) {
      int credentialNumber = i + 1;
      response.addStorageCredentialsItem(
          new DeltaStorageCredential()
              .prefix(prefixes[i])
              .operation(DeltaCredentialOperation.READ_WRITE)
              .expirationTimeMs((long) credentialNumber)
              .config(
                  new DeltaStorageCredentialConfig()
                      .s3AccessKeyId("ak" + credentialNumber)
                      .s3SecretAccessKey("sk" + credentialNumber)
                      .s3SessionToken("st" + credentialNumber)));
    }
    return response;
  }

  static void assertAwsCredentialsInOrder(List<GenericCredential> credentials, String... prefixes) {
    assertThat(credentials).hasSize(prefixes.length);
    for (int i = 0; i < prefixes.length; i++) {
      AwsCredential credential = (AwsCredential) credentials.get(i);
      assertThat(credential.accessKeyId()).isEqualTo("ak" + (i + 1));
      assertThat(credential.prefix()).isEqualTo(prefixes[i]);
    }
  }

  static void assertRejectsNullOrEmptyStorageCredentials(
      DeltaCredentialsResponse response, ThrowingCallable fetch) {
    when(response.getStorageCredentials()).thenReturn(null);
    assertThatThrownBy(fetch)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no storage credentials");

    when(response.getStorageCredentials()).thenReturn(List.of());
    assertThatThrownBy(fetch)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no storage credentials");
  }
}
