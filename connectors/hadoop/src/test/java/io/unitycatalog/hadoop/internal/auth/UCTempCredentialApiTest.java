package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class UCTempCredentialApiTest {

  @Test
  void factoryRequiresUcUri() {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf();
    conf.unset(UCHadoopConfConstants.UC_URI_KEY);

    assertThatThrownBy(() -> TempCredentialApi.create(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConfConstants.UC_URI_KEY);
  }

  @Test
  void factoryRequiresTokenProviderConfig() {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf();
    conf.unset(UCHadoopConfConstants.UC_AUTH_TYPE);
    conf.unset(UCHadoopConfConstants.UC_AUTH_TOKEN_KEY);

    assertThatThrownBy(() -> TempCredentialApi.create(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Required configuration key 'type' is missing or empty");
  }

  @Test
  void tableRequestArgumentsAreParsedOnceAtConstruction() throws Exception {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf("original-table-id");
    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.generateTemporaryTableCredentials(any())).thenReturn(new TemporaryCredentials());

    TempCredentialApi credentialApi = new UCTempCredentialApi(conf, api);

    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "mutated-table-id");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "UNKNOWN");
    conf.set(UCHadoopConfConstants.UC_PATH_KEY, "s3://mutated/path");
    conf.set(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, PathOperation.PATH_READ_WRITE.getValue());

    credentialApi.createCredential();

    ArgumentCaptor<GenerateTemporaryTableCredential> request =
        ArgumentCaptor.forClass(GenerateTemporaryTableCredential.class);
    verify(api).generateTemporaryTableCredentials(request.capture());
    verify(api, never()).generateTemporaryPathCredentials(any());
    assertThat(request.getValue().getTableId()).isEqualTo("original-table-id");
    assertThat(request.getValue().getOperation()).isEqualTo(TableOperation.READ);
  }

  @Test
  void pathRequestArgumentsAreParsedOnceAtConstruction() throws Exception {
    Configuration conf = BaseTokenProviderTest.newPathBasedConf("s3://bucket/original-path");
    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.generateTemporaryPathCredentials(any())).thenReturn(new TemporaryCredentials());

    TempCredentialApi credentialApi = new UCTempCredentialApi(conf, api);

    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_PATH_KEY, "s3://bucket/mutated-path");
    conf.set(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "UNKNOWN");
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "mutated-table-id");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ_WRITE.getValue());

    credentialApi.createCredential();

    ArgumentCaptor<GenerateTemporaryPathCredential> request =
        ArgumentCaptor.forClass(GenerateTemporaryPathCredential.class);
    verify(api).generateTemporaryPathCredentials(request.capture());
    verify(api, never()).generateTemporaryTableCredentials(any());
    assertThat(request.getValue().getUrl()).isEqualTo("s3://bucket/original-path");
    assertThat(request.getValue().getOperation()).isEqualTo(PathOperation.PATH_READ);
  }
}
