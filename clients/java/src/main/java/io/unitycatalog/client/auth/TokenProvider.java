package io.unitycatalog.client.auth;

import com.google.common.base.Preconditions;
import java.util.Map;

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

  static TokenProvider create(String token) {
    return new FixedTokenProvider(token);
  }

  static TokenProvider create(String oauthUri, String oauthClientId, String oauthClientSecret) {
    return new OAuthTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
  }

  static TokenProvider create(Map<String, String> options) {
    String token = options.get(AuthProps.TOKEN);
    String oauthUri = options.get(AuthProps.OAUTH_URI);
    String oauthClientId = options.get(AuthProps.OAUTH_CLIENT_ID);
    String oauthClientSecret = options.get(AuthProps.OAUTH_CLIENT_SECRET);

    if (token != null) {
      Preconditions.checkArgument(
          oauthUri == null && oauthClientId == null && oauthClientSecret == null,
          "Invalid Unity Catalog authentication configuration: token-based and OAuth " +
              "settings were both supplied. Configure exactly one authentication method.");
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
        "Cannot determine unity catalog authentication "
            + "configuration from options, please set token for static token authentication or "
            + "oauth.uri, oauth.clientId, oauth.clientSecret for OAuth 2.0 authentication "
            + "(all three required)");
  }
}
