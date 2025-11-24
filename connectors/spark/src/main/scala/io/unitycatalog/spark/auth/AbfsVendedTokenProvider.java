package io.unitycatalog.spark.auth;

import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.spark.UCHadoopConf;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.base.Preconditions;

public class AbfsVendedTokenProvider extends GenericCredentialProvider implements SASTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  public AbfsVendedTokenProvider() {
  }

  @Override
  public void initialize(Configuration conf, String accountName) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.AZURE_INIT_SAS_TOKEN) != null) {

      String sasToken = conf.get(UCHadoopConf.AZURE_INIT_SAS_TOKEN);
      Preconditions.checkNotNull(sasToken, "Azure SAS token not set, please check " +
          "'%s' in hadoop configuration", UCHadoopConf.AZURE_INIT_SAS_TOKEN);

      long expiredTimeMillis = conf.getLong(
          UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
          Long.MAX_VALUE);
      Preconditions.checkState(expiredTimeMillis > 0,
          "Azure SAS token expired time must be greater than 0, please check '%s' in hadoop " +
              "configuration", UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME);

      return GenericCredential.forAzure(sasToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    GenericCredential generic = accessCredentials();

    AzureUserDelegationSAS azureSAS = generic.temporaryCredentials().getAzureUserDelegationSas();
    Preconditions.checkNotNull(azureSAS, "Azure SAS of generic credential cannot be null");

    return azureSAS.getSasToken();
  }
}
