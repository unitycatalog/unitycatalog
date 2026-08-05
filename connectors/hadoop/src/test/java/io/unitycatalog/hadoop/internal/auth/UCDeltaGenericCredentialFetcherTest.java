package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.auth.UCDeltaCredentialFetcherTestUtils.assertAwsCredentialsInOrder;
import static io.unitycatalog.hadoop.internal.auth.UCDeltaCredentialFetcherTestUtils.assertRejectsNullOrEmptyStorageCredentials;
import static io.unitycatalog.hadoop.internal.auth.UCDeltaCredentialFetcherTestUtils.s3Response;
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
  void createCredentialsCallsDeltaApiWithCredIdFieldsAndReturnsCredential() throws Exception {
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
    assertThat(cred.prefix()).isEqualTo("s3://bucket/events");
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

    DeltaCredentialsResponse response =
        s3Response("s3://bucket/events", "s3://bucket/events/child");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    List<GenericCredential> credentials =
        GenericCredentialFetcher.forUcDelta(credId, api).createCredentials();
    assertAwsCredentialsInOrder(credentials, "s3://bucket/events", "s3://bucket/events/child");
  }

  @Test
  void createCredentialsRejectsMissingDeltaCredentialsResponse() throws Exception {
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
  void createCredentialsRejectsNullOrEmptyStorageCredentials() throws Exception {
    DeltaTableCredId credId =
        new DeltaTableCredId(
            EMPTY_CRED_CONTEXT_ID,
            UCDeltaTableIdentifier.of("main", "default", "events"),
            "READ_WRITE",
            "s3://bucket/events");

    DeltaCredentialsResponse response = mock(DeltaCredentialsResponse.class);
    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    assertRejectsNullOrEmptyStorageCredentials(
        response, () -> GenericCredentialFetcher.forUcDelta(credId, api).createCredentials());
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
