package io.unitycatalog.spark.auth;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class AbfsFixedTokenProvider implements SASTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";
  private Configuration conf;

  @Override
  public void initialize(Configuration conf, String accountName) throws IOException {
    this.conf = conf;
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation){
    System.out.print("===> " + ACCESS_TOKEN_KEY + " --> " + conf.get(ACCESS_TOKEN_KEY));
    return conf.get(ACCESS_TOKEN_KEY);
  }
}
