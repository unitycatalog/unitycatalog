package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UCDeltaStagingTableCredentialFetcherTest {

  private static final UUID STAGING_ID = UUID.randomUUID();
  private static final String LOCATION = "s3://bucket/staging-table";

  @Test
  void createCredentialCallsDeltaStagingApiAndReturnsCredential() throws Exception {
    DeltaStagingTableCredId credId = new DeltaStagingTableCredId(STAGING_ID.toString(), LOCATION);
    CredentialsResponse response = s3StagingResponse();

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    GenericCredential cred =
        GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredential();

    assertThat(cred).isNotNull();
    TemporaryCredentials out = cred.temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(1234L);
    verify(api).getStagingTableCredentials(STAGING_ID);
  }

  @Test
  void createCredentialRejectsNullResponse() throws Exception {
    DeltaStagingTableCredId credId = new DeltaStagingTableCredId(STAGING_ID.toString(), LOCATION);

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(null);

    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredential())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("returned no credentials response");
  }

  private static CredentialsResponse s3StagingResponse() {
    StorageCredential sc =
        new StorageCredential()
            .prefix(LOCATION)
            .operation(CredentialOperation.READ_WRITE)
            .expirationTimeMs(1234L)
            .config(
                new StorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    return new CredentialsResponse().addStorageCredentialsItem(sc);
  }
}
