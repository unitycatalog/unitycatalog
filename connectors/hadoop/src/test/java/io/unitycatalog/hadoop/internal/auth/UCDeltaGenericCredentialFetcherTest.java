package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import org.junit.jupiter.api.Test;

class UCDeltaGenericCredentialFetcherTest {

  @Test
  void createCredentialCallsDeltaApiWithCredIdFieldsAndReturnsCredential() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            EMPTY_CRED_CONTEXT_ID,
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    DeltaStorageCredential sc =
        new DeltaStorageCredential()
            .prefix("s3://bucket/events")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(789L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    DeltaCredentialsResponse response =
        new DeltaCredentialsResponse().addStorageCredentialsItem(sc);

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    AwsCredential cred =
        (AwsCredential) GenericCredentialFetcher.forUcDelta(credId, api).createCredential();

    assertThat(cred.accessKeyId()).isEqualTo("ak");
    assertThat(cred.secretAccessKey()).isEqualTo("sk");
    assertThat(cred.sessionToken()).isEqualTo("st");
    assertThat(cred.expirationTimeMillis()).isEqualTo(789L);
    verify(api)
        .getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialRejectsMissingDeltaCredentialsResponse() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            EMPTY_CRED_CONTEXT_ID,
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(null);

    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(credId, api).createCredential())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void factoryRejectsUnsupportedTableOperation() {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            EMPTY_CRED_CONTEXT_ID, UCDeltaTableIdentifier.of("c", "s", "n"), "UNKNOWN", "s3://b/p");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(credId, api))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
