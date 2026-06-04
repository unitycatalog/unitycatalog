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
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class UCDeltaStagingTableCredentialFetcherTest {

  private static final UUID STAGING_ID = UUID.randomUUID();
  private static final String LOCATION = "s3://bucket/staging-table";

  @Test
  void createCredentialCallsDeltaStagingApiAndReturnsCredential() throws Exception {
    Configuration conf = stagingConf();
    DeltaCredentialsResponse response = s3StagingResponse();

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    GenericCredential cred =
        GenericCredentialFetcher.forUcDeltaStagingTable(conf, api).createCredential();

    assertThat(cred).isNotNull();
    TemporaryCredentials out = cred.temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(1234L);
    verify(api).getStagingTableCredentials(STAGING_ID);
  }

  @Test
  void createCredentialUsesFieldsParsedAtConstruction() throws Exception {
    Configuration conf = stagingConf();
    DeltaCredentialsResponse response = s3StagingResponse();

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);
    GenericCredentialFetcher fetcher = GenericCredentialFetcher.forUcDeltaStagingTable(conf, api);

    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, UUID.randomUUID().toString());
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "s3://bucket/mutated");

    fetcher.createCredential();

    verify(api).getStagingTableCredentials(STAGING_ID);
  }

  @Test
  void createCredentialRejectsNullResponse() throws Exception {
    Configuration conf = stagingConf();

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(null);

    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(conf, api).createCredential())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("returned no credentials response");
  }

  @Test
  void factoryThrowsWhenConfMissingStagingTableId() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, LOCATION);

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDeltaStagingTable(conf, api))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY);
  }

  @Test
  void factoryThrowsWhenConfMissingLocation() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, STAGING_ID.toString());

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    assertThatThrownBy(() -> GenericCredentialFetcher.forUcDeltaStagingTable(conf, api))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY);
  }

  private static Configuration stagingConf() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, STAGING_ID.toString());
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, LOCATION);
    return conf;
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
