package io.unitycatalog.spark.auth;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;

import java.io.IOException;
import java.time.Instant;

public class GcsVendedTokenProvider extends GenericCredentialProvider
    implements AccessTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  public static final String ACCESS_TOKEN_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";

  private Configuration conf;

  /**
   * Constructor for the hadoop's CredentialProviderListFactory to initialize.
   */
  public GcsVendedTokenProvider() {
    super();
  }

  GcsVendedTokenProvider(Clock clock, long renewalLeadTimeMillis) {
    super(clock, renewalLeadTimeMillis);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCHadoopConf.GCS_INIT_OAUTH_TOKEN) != null
        && conf.get(UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME) != null) {

      String oauthToken = conf.get(UCHadoopConf.GCS_INIT_OAUTH_TOKEN);
      long expiredTimeMillis = conf.getLong(
          UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME,
          Long.MAX_VALUE);
      Preconditions.checkState(expiredTimeMillis > 0, "Expired time %s must be greater than 0, " +
          "please check configure key '%s'", expiredTimeMillis, 
          UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME);

      return GenericCredential.forGcs(oauthToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public AccessToken getAccessToken() {
    GenericCredential generic = accessCredentials();

    // Wrap the GenericCredential as an AccessToken.
    GcpOauthToken gcpToken = generic
        .temporaryCredentials()
        .getGcpOauthToken();
    Preconditions.checkNotNull(gcpToken,
        "GCP OAuth token of generic credentials cannot be null");

    return new AccessToken(
        gcpToken.getOauthToken(),
        Instant.ofEpochMilli(generic.temporaryCredentials().getExpirationTime())
    );
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

