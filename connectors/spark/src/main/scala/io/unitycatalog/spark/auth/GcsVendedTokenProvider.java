package io.unitycatalog.spark.auth;

import io.unitycatalog.spark.UCHadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;
import io.unitycatalog.client.model.GcpOauthToken;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;

import java.io.IOException;
import java.time.Instant;

public class GcsVendedTokenProvider extends GenericCredentialProvider
    implements AccessTokenProvider {

  public static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  public static final String ACCESS_TOKEN_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";

  private Configuration conf;

  public GcsVendedTokenProvider() {
  }

  public GcsVendedTokenProvider(Configuration conf) {
    setConf(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.GCS_INIT_OAUTH_TOKEN) != null) {
      String oauthToken = conf.get(UCHadoopConf.GCS_INIT_OAUTH_TOKEN);
      Preconditions.checkNotNull(oauthToken, "GCS OAuth token not set, please check '%s' in hadoop "
          + "configuration", UCHadoopConf.GCS_INIT_OAUTH_TOKEN);

      long expiredTimeMillis = conf.getLong(
            UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME,
            Long.MAX_VALUE);
      Preconditions.checkState(expiredTimeMillis > 0, "Expired time %s must be greater than 0, " +
          "please check configure key '%s'", expiredTimeMillis, UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME);

      return GenericCredential.forGcs(oauthToken, expiredTimeMillis);
    } else {
      return null;
    }
  }


  @Override
  public AccessToken getAccessToken() {
    GenericCredential generic = accessCredentials();

    GcpOauthToken gcpToken = generic.temporaryCredentials().getGcpOauthToken();
    Preconditions.checkNotNull(gcpToken, "GCS OAuth token of generic credential cannot be null");

    String tokenValue = gcpToken.getOauthToken();
    Preconditions.checkNotNull(tokenValue, "GCS OAuth token value cannot be null");

    Long expirationMillis = generic.temporaryCredentials().getExpirationTime();
    Instant expirationInstant = expirationMillis == null
        ? null
        : Instant.ofEpochMilli(expirationMillis);
    return new AccessToken(tokenValue, expirationInstant);
  }

  @Override
  public void refresh() throws IOException {
    // The refresh is handled by accessCredentials() in the parent class.
    // This method is called by the GCS connector, but we don't need to do anything here.
    // The actual renewal happens automatically when getAccessToken() is called.
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    initialize(configuration);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}

