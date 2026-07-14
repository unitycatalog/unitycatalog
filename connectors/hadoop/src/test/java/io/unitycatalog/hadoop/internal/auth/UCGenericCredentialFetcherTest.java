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
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class UCGenericCredentialFetcherTest {

  @Test
  void factoryRequiresUcUri() {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf();
    conf.unset(UCHadoopConfConstants.UC_URI_KEY);

    assertThatThrownBy(() -> GenericCredentialFetcher.create(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining(UCHadoopConfConstants.UC_URI_KEY);
  }

  @Test
  void factoryRequiresTokenProviderConfig() {
    Configuration conf = BaseTokenProviderTest.newTableBasedConf();
    conf.unset(UCHadoopConfConstants.UC_AUTH_TYPE);
    conf.unset(UCHadoopConfConstants.UC_AUTH_TOKEN_KEY);

    assertThatThrownBy(() -> GenericCredentialFetcher.create(conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Required configuration key 'type' is missing or empty");
  }

  @Test
  void tableRequestBuiltFromCredId() throws Exception {
    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.generateTemporaryTableCredentials(any())).thenReturn(new TemporaryCredentials());

    GenericCredentialFetcher credentialFetcher =
        GenericCredentialFetcher.forUc(
            new TableCredId(
                CredId.EMPTY_CRED_CONTEXT_ID, "original-table-id", TableOperation.READ.getValue()),
            api);

    credentialFetcher.createCredential();

    ArgumentCaptor<GenerateTemporaryTableCredential> request =
        ArgumentCaptor.forClass(GenerateTemporaryTableCredential.class);
    verify(api).generateTemporaryTableCredentials(request.capture());
    verify(api, never()).generateTemporaryPathCredentials(any());
    assertThat(request.getValue().getTableId()).isEqualTo("original-table-id");
    assertThat(request.getValue().getOperation()).isEqualTo(TableOperation.READ);
  }

  @Test
  void pathRequestBuiltFromCredId() throws Exception {
    TemporaryCredentialsApi api = mock(TemporaryCredentialsApi.class);
    when(api.generateTemporaryPathCredentials(any())).thenReturn(new TemporaryCredentials());

    GenericCredentialFetcher credentialFetcher =
        GenericCredentialFetcher.forUc(
            new PathCredId(
                CredId.EMPTY_CRED_CONTEXT_ID,
                "s3://bucket/original-path",
                PathOperation.PATH_READ.getValue()),
            api);

    credentialFetcher.createCredential();

    ArgumentCaptor<GenerateTemporaryPathCredential> request =
        ArgumentCaptor.forClass(GenerateTemporaryPathCredential.class);
    verify(api).generateTemporaryPathCredentials(request.capture());
    verify(api, never()).generateTemporaryTableCredentials(any());
    assertThat(request.getValue().getUrl()).isEqualTo("s3://bucket/original-path");
    assertThat(request.getValue().getOperation()).isEqualTo(PathOperation.PATH_READ);
  }
}
