package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import org.junit.jupiter.api.Test;

class UCDeltaGenericCredentialFetcherTest {

  @Test
  void createCredentialCallsDeltaApiWithCredIdFieldsAndReturnsCredential() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    StorageCredential sc =
        new StorageCredential()
            .prefix("s3://bucket/events")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(789L)
            .config(
                new StorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    CredentialsResponse response = new CredentialsResponse().addStorageCredentialsItem(sc);

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    GenericCredential cred = GenericCredentialFetcher.forUcDelta(credId, api).createCredential();

    assertThat(cred).isNotNull();
    TemporaryCredentials out = cred.temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(789L);
    verify(api).getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialRejectsMissingCredentialsResponse() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(null);

    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(credId, api).createCredential())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void factoryRejectsUnsupportedTableOperation() {
    DeltaTableCredId credId =
        new DeltaTableCredId(UCDeltaTableIdentifier.of("c", "s", "n"), "UNKNOWN", "s3://b/p");

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(credId, api))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
