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
import java.util.List;
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

    List<GenericCredential> creds =
        GenericCredentialFetcher.forUcDelta(credId, api).createCredentials();

    assertThat(creds).hasSize(1);
    AwsCredential cred = (AwsCredential) creds.get(0);
    assertThat(cred.accessKeyId()).isEqualTo("ak");
    assertThat(cred.secretAccessKey()).isEqualTo("sk");
    assertThat(cred.sessionToken()).isEqualTo("st");
    assertThat(cred.expirationTimeMillis()).isEqualTo(789L);
    assertThat(cred.location()).isEqualTo("s3://bucket/events");
    verify(api)
        .getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialsReturnsAllVendedCredentialsInOrder() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            EMPTY_CRED_CONTEXT_ID,
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    DeltaStorageCredential first =
        new DeltaStorageCredential()
            .prefix("s3://bucket/events")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(1L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak1")
                    .s3SecretAccessKey("sk1")
                    .s3SessionToken("st1"));
    DeltaStorageCredential second =
        new DeltaStorageCredential()
            .prefix("s3://bucket/events/child")
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
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    List<GenericCredential> creds =
        GenericCredentialFetcher.forUcDelta(credId, api).createCredentials();

    assertThat(creds).hasSize(2);
    AwsCredential cred1 = (AwsCredential) creds.get(0);
    assertThat(cred1.accessKeyId()).isEqualTo("ak1");
    assertThat(cred1.location()).isEqualTo("s3://bucket/events");
    AwsCredential cred2 = (AwsCredential) creds.get(1);
    assertThat(cred2.accessKeyId()).isEqualTo("ak2");
    assertThat(cred2.location()).isEqualTo("s3://bucket/events/child");
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

    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(credId, api).createCredentials())
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
