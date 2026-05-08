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
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class UCDeltaTempCredentialApiTest {

  @Test
  void createCredentialCallsDeltaApiWithConfFieldsAndReturnsCredential() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

    StorageCredential sc =
        new StorageCredential()
            .prefix("s3://bucket/events")
            .operation(CredentialOperation.READ_WRITE)
            .expirationTimeMs(789L)
            .config(
                new StorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    CredentialsResponse response = new CredentialsResponse().addStorageCredentialsItem(sc);

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getTableCredentials(CredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);

    GenericCredential cred = new UCDeltaTempCredentialApi(conf, api).createCredential();

    assertThat(cred).isNotNull();
    TemporaryCredentials out = cred.temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(789L);
    verify(api).getTableCredentials(CredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialUsesFieldsParsedAtConstruction() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

    StorageCredential sc =
        new StorageCredential()
            .prefix("s3://bucket/events")
            .operation(CredentialOperation.READ_WRITE)
            .config(
                new StorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    CredentialsResponse response = new CredentialsResponse().addStorageCredentialsItem(sc);

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getTableCredentials(CredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(response);
    TempCredentialApi credentialApi = new UCDeltaTempCredentialApi(conf, api);

    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "mutated-catalog");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "mutated-schema");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "mutated-name");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/mutated");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "UNKNOWN");

    credentialApi.createCredential();

    verify(api).getTableCredentials(CredentialOperation.READ_WRITE, "main", "default", "events");
  }

  @Test
  void createCredentialRejectsMissingCredentialsResponse() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "main");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "default");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "events");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/events");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.getTableCredentials(CredentialOperation.READ_WRITE, "main", "default", "events"))
        .thenReturn(null);

    assertThatThrownBy(() -> new UCDeltaTempCredentialApi(conf, api).createCredential())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void constructorThrowsWhenConfMissingCatalog() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "s");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "n");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    assertThatThrownBy(() -> new UCDeltaTempCredentialApi(conf, api))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fs.unitycatalog.delta.catalog");
  }

  @Test
  void constructorRejectsUnsupportedTableOperation() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "c");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "s");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "n");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "UNKNOWN");

    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    assertThatThrownBy(() -> new UCDeltaTempCredentialApi(conf, api))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tempCredentialApiRejectsInvalidDeltaApiEnabledValue() {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf();
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "other");

    assertThatThrownBy(() -> TempCredentialApi.create(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY);
  }
}
