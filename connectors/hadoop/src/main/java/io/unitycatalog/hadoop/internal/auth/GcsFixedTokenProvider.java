package io.unitycatalog.hadoop.internal.auth;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;

/**
 * Hadoop GCS access token provider that serves the token Unity Catalog vended when credential
 * renewal is disabled. The GCS connector has no setting that takes a token directly, so it needs a
 * provider even for a token that is never renewed. Once the token expires, {@link #refresh()} fails
 * rather than renewing it.
 */
public class GcsFixedTokenProvider implements AccessTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  public static final String ACCESS_TOKEN_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";

  private Configuration conf;
  private Clock clock;
  private AccessToken accessToken;

  @Override
  public AccessToken getAccessToken() {
    return accessToken;
  }

  /**
   * Does nothing while the token is valid, since the connector also refreshes a token shortly
   * before it expires.
   *
   * @throws IOException if the token has expired
   */
  @Override
  public void refresh() throws IOException {
    if (!clock.now().isBefore(accessToken.getExpirationTime())) {
      throw new IOException(
          String.format(
              "The GCS token vended by Unity Catalog expired at %s and credential renewal is"
                  + " disabled, so it cannot be renewed. Load the table or path again to get a new"
                  + " token, or enable credential renewal.",
              accessToken.getExpirationTime()));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.clock = ClockUtil.resolveClock(conf);
    this.accessToken =
        new AccessToken(
            conf.get(ACCESS_TOKEN_KEY),
            Instant.ofEpochMilli(conf.getLong(ACCESS_TOKEN_EXPIRATION_KEY, Long.MAX_VALUE)));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
