package io.unitycatalog.spark.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.mockito.stubbing.OngoingStubbing;

public class ProviderBuilder {
  private Configuration conf;
  private long renewalLeadTimeMillis;
  private final List<TemporaryCredentials> tableCreds = new ArrayList<>();
  private final List<TemporaryCredentials> pathCreds = new ArrayList<>();

  public ProviderBuilder setConf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  public ProviderBuilder setRenewalLeadTimeMillis(long renewalLeadTimeMillis) {
    this.renewalLeadTimeMillis = renewalLeadTimeMillis;
    return this;
  }

  public ProviderBuilder addTableCred(TemporaryCredentials... creds) {
    Collections.addAll(tableCreds, creds);
    return this;
  }

  public ProviderBuilder addPathCred(TemporaryCredentials... creds) {
    Collections.addAll(pathCreds, creds);
    return this;
  }

  private AwsVendedTokenProvider build() throws ApiException {
    TemporaryCredentialsApi api = new TemporaryCredentialsApi();

    // Mock the table credential sequence.
    OngoingStubbing<TemporaryCredentials> tableChain =
        when(api.generateTemporaryTableCredentials(any()));
    for (TemporaryCredentials cred : tableCreds) {
      tableChain.thenReturn(cred);
    }

    // Mock the path credential sequence.
    OngoingStubbing<TemporaryCredentials> pathChain =
        when(api.generateTemporaryPathCredentials(any()));
    for (TemporaryCredentials cred : pathCreds) {
      pathChain.thenReturn(cred);
    }

    return new AwsVendedTokenProvider(conf) {
      @Override
      protected TemporaryCredentialsApi temporaryCredentialsApi() {
        return api;
      }
    };
  }

  public static ProviderBuilder builder() {
    return new ProviderBuilder();
  }
}
