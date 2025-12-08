package io.unitycatalog.client.auth;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Interface for providing access tokens to authenticate with Unity Catalog.
 *
 * <p>Implementations include:
 *
 * <ul>
 *   <li>{@link FixedTokenProvider} - uses a pre-configured static token
 *   <li>{@link OAuthTokenProvider} - obtains tokens via OAuth 2.0 client credentials flow
 * </ul>
 */
public interface TokenProvider {
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

  static Builder builder() {
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

    private void setIfPresent(Map<String, String> options, String key, Consumer<String> setter) {
      String value = options.get(key);
      if (value != null) {
        setter.accept(value);
      }
    }

    public Builder options(Map<String, String> options) {
      setIfPresent(options, AuthProps.TOKEN, this::token);
      setIfPresent(options, AuthProps.OAUTH_URI, this::oauthUri);
      setIfPresent(options, AuthProps.OAUTH_CLIENT_ID, this::oauthClientId);
      setIfPresent(options, AuthProps.OAUTH_CLIENT_SECRET, this::oauthClientSecret);
      return this;
    }

    public TokenProvider build() {
      if (token != null) {
        return new FixedTokenProvider(token);
      }

      if (oauthUri != null || oauthClientId != null || oauthClientSecret != null) {
        Preconditions.checkArgument(
            oauthUri != null && oauthClientId != null && oauthClientSecret != null,
            "Incomplete OAuth configuration detected. All of the keys are required: "
                + "oauthUri, oauthClientId, oauthClientSecret. Please ensure they are "
                + "all set.");

        return new OAuthTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
      }

      throw new IllegalArgumentException(
          "Cannot determine UC authentication "
              + "configuration from options, please set %stoken for static token authentication or "
              + "oauth.uri, oauth.clientId, oauth.clientSecret for OAuth 2.0 authentication "
              + "(all three required)");
    }
  }
}
