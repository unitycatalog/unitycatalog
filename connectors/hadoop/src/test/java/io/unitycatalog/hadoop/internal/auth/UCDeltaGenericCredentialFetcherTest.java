package io.unitycatalog.hadoop.internal.auth;

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
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class UCDeltaGenericCredentialFetcherTest {

  @Test
  void createCredentialCallsDeltaApiWithConfFieldsAndReturnsCredential() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

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

    GenericCredential cred = GenericCredentialFetcher.forUcDelta(conf, api).createCredential();

    assertThat(cred).isNotNull();
    TemporaryCredentials out = cred.temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(789L);
    verify(api)
        .getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialUsesFieldsParsedAtConstruction() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

    DeltaStorageCredential sc =
        new DeltaStorageCredential()
            .prefix("s3://bucket/events")
            .operation(DeltaCredentialOperation.READ_WRITE)
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
    GenericCredentialFetcher credentialFetcher = GenericCredentialFetcher.forUcDelta(conf, api);

    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "mutated-catalog");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "mutated-schema");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "mutated-name");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/mutated");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "UNKNOWN");

    credentialFetcher.createCredential();

    verify(api)
        .getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialRejectsMissingCredentialsResponse() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getTableCredentials(DeltaCredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(null);

    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(conf, api).createCredential())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void factoryThrowsWhenConfMissingCatalog() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "s");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "n");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(conf, api))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fs.unitycatalog.delta.catalog");
  }

  @Test
  void factoryRejectsUnsupportedTableOperation() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "c");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "s");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "n");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "UNKNOWN");

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDelta(conf, api))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
