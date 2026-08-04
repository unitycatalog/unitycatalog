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
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UCDeltaStagingTableCredentialFetcherTest {

  private static final UUID STAGING_ID = UUID.randomUUID();
  private static final String LOCATION = "s3://bucket/staging-table";

  @Test
  void createCredentialsCallsDeltaStagingApiAndReturnsCredential() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);
    DeltaCredentialsResponse response = s3StagingResponse();

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    List<GenericCredential> creds =
        GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials();

    assertThat(creds).hasSize(1);
    AwsCredential cred = (AwsCredential) creds.get(0);
    assertThat(cred.accessKeyId()).isEqualTo("ak");
    assertThat(cred.secretAccessKey()).isEqualTo("sk");
    assertThat(cred.sessionToken()).isEqualTo("st");
    assertThat(cred.expirationTimeMillis()).isEqualTo(1234L);
    assertThat(cred.prefix()).isEqualTo(LOCATION);
    verify(api).getStagingTableCredentials(STAGING_ID);
  }

  @Test
  void createCredentialsReturnsAllVendedCredentialsInOrder() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);

    DeltaStorageCredential first =
        new DeltaStorageCredential()
            .prefix(LOCATION)
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(1L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak1")
                    .s3SecretAccessKey("sk1")
                    .s3SessionToken("st1"));
    DeltaStorageCredential second =
        new DeltaStorageCredential()
            .prefix(LOCATION + "/child")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(2L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak2")
                    .s3SecretAccessKey("sk2")
                    .s3SessionToken("st2"));
    DeltaCredentialsResponse response =
        new DeltaCredentialsResponse()
            .addStorageCredentialsItem(first)
            .addStorageCredentialsItem(second);

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    List<GenericCredential> creds =
        GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials();

    assertThat(creds).hasSize(2);
    AwsCredential cred1 = (AwsCredential) creds.get(0);
    assertThat(cred1.accessKeyId()).isEqualTo("ak1");
    assertThat(cred1.prefix()).isEqualTo(LOCATION);
    AwsCredential cred2 = (AwsCredential) creds.get(1);
    assertThat(cred2.accessKeyId()).isEqualTo("ak2");
    assertThat(cred2.prefix()).isEqualTo(LOCATION + "/child");
  }

  @Test
  void createCredentialsRejectsNullResponse() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(null);

    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void createCredentialsRejectsNullOrEmptyStorageCredentials() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);

    DeltaCredentialsResponse response = mock(DeltaCredentialsResponse.class);
    when(response.getStorageCredentials()).thenReturn(null).thenReturn(List.of());
    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no storage credentials");
    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no storage credentials");
  }

  private static DeltaCredentialsResponse s3StagingResponse() {
    DeltaStorageCredential sc =
        new DeltaStorageCredential()
            .prefix(LOCATION)
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(1234L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    return new DeltaCredentialsResponse().addStorageCredentialsItem(sc);
  }
}
