package io.unitycatalog.server.persist.utils.hdfs.gcs;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;

public class GCSAccessTokenProvider implements AccessTokenProvider {
  private String accessToken;
  // Default expiration time: 1 hour from now
  private static final long DEFAULT_EXPIRATION_DURATION_MS = 3600 * 1000; // 1 hour in milliseconds

  private Configuration conf;

  public GCSAccessTokenProvider() {}

  @Override
  public AccessToken getAccessToken() {
    return new AccessToken(
        accessToken,
        new Date().toInstant().plusMillis(DEFAULT_EXPIRATION_DURATION_MS).toEpochMilli());
  }

  @Override
  public void refresh() throws IOException {
    // No-op
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    this.accessToken = configuration.get("google.cloud.auth.access.token.value");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
