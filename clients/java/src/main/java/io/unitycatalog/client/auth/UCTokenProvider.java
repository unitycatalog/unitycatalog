package io.unitycatalog.client.auth;

import io.unitycatalog.client.Preconditions;
import java.util.Map;

/**
 * Interface for providing access tokens to authenticate with Unity Catalog.
 *
 * <p>Implementations include:
 * <ul>
 *   <li>{@link FixedUCTokenProvider} - uses a pre-configured static token</li>
 *   <li>{@link OAuthUCTokenProvider} - obtains tokens via OAuth 2.0 client credentials flow</li>
 * </ul>
 */
public interface UCTokenProvider {
  /**
   * Returns the access token for Unity Catalog authentication.
   *
   * @return the access token string
   */
  String accessToken();

  /**
   * Returns the configuration properties associated with this token provider.
   *
   * @return a map of configuration key-value pairs
   */
  Map<String, String> properties();

  static Builder newBuilder() {
    return new Builder();
  }

  class Builder {
    private String token;
    private String oauthUri;
    private String oauthClientId;
    private String oauthClientSecret;

    public Builder token(String token) {
      this.token = token;
      return this;
    }

    public Builder oauthUri(String oauthUri) {
      this.oauthUri = oauthUri;
      return this;
    }

    public Builder oauthClientId(String oauthClientId) {
      this.oauthClientId = oauthClientId;
      return this;
    }

    public Builder oauthClientSecret(String oauthClientSecret) {
      this.oauthClientSecret = oauthClientSecret;
      return this;
    }

    public UCTokenProvider build() {
      if (token != null) {
        return new FixedUCTokenProvider(token);
      }

      if (oauthUri != null || oauthClientId != null || oauthClientSecret != null) {
        Preconditions.checkArgument(
            oauthUri != null && oauthClientId != null && oauthClientSecret != null,
            "Incomplete OAuth configuration detected. All of the keys are required: " +
                "oauthUri, oauthClientId, oauthClientSecret. Please ensure they are " +
                "all set.");

        return new OAuthUCTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
      }

      throw new IllegalArgumentException("Cannot determine UC authentication " +
          "configuration from options, please set %stoken for static token authentication or " +
          "oauth.uri, oauth.clientId, oauth.clientSecret for OAuth 2.0 authentication " +
          "(all three required)");
    }
  }
}
