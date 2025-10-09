package io.unitycatalog.spark.auth;

import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.spark.UCHadoopConf;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.base.Preconditions;

public class AbfsVendedTokenProvider extends GenericCredentialProvider implements SASTokenProvider {

  @Override
  public void initialize(Configuration conf, String accountName) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.AZURE_SAS_TOKEN) != null
        && conf.get(UCHadoopConf.AZURE_SAS_TOKEN_EXPIRED_TIME) != null) {

      String azureSas = conf.get(UCHadoopConf.AZURE_SAS_TOKEN);
      Preconditions.checkNotNull(azureSas, "Azure SAS token not set, please check configure key " +
          "'%s' in hadoop configuration", UCHadoopConf.AZURE_SAS_TOKEN);

      long expiredTimeMillis = conf.getLong(UCHadoopConf.AZURE_SAS_TOKEN_EXPIRED_TIME, 0L);
      Preconditions.checkState(expiredTimeMillis > 0,
          "Azure SAS token expired time must be greater than 0");

      return GenericCredential.forAzure(azureSas, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    GenericCredential generic = accessCredentials();

    AzureUserDelegationSAS azureSAS = generic.temporaryCredentials().getAzureUserDelegationSas();
    Preconditions.checkNotNull(azureSAS,
        "Azure SAS of generic credential cannot be null");

    return azureSAS.getSasToken();
  }
}
