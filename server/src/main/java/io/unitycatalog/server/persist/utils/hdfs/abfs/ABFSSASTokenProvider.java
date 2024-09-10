package io.unitycatalog.server.persist.utils.hdfs.abfs;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class ABFSSASTokenProvider implements SASTokenProvider {
  private String fixedSASToken;

  public ABFSSASTokenProvider(final String fixedSASToken) throws SASTokenProviderException {
    this.fixedSASToken = fixedSASToken;
    if (fixedSASToken == null || fixedSASToken.isEmpty()) {
      throw new SASTokenProviderException(
          String.format("Configured Fixed SAS Token is Invalid: %s", fixedSASToken));
    }
  }

  public ABFSSASTokenProvider() {
    this.fixedSASToken = null;
  }

  @Override
  public void initialize(final Configuration configuration, final String accountName)
      throws IOException {
    Map<String, String> tokenMap = configuration.getPropsWithPrefix("fs.azure.sas.fixed.token");
    if (tokenMap.size() != 1) {
      throw new IOException("Exactly one fixed SAS token should be configured");
    }
    fixedSASToken = tokenMap.values().iterator().next();
    System.out.println("Initializing FixedSASTokenProvider");
  }

  /**
   * Returns the fixed SAS Token configured.
   *
   * @param account the name of the storage account.
   * @param fileSystem the name of the fileSystem.
   * @param path the file or directory path.
   * @param operation the operation to be performed on the path.
   * @return Fixed SAS Token
   * @throws IOException never
   */
  @Override
  public String getSASToken(
      final String account, final String fileSystem, final String path, final String operation)
      throws IOException {
    return fixedSASToken;
  }
}
