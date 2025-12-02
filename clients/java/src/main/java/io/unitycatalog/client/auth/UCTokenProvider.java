package io.unitycatalog.client.auth;

import io.unitycatalog.client.Constants;
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

  /**
   * Creates a token provider from configuration options. Returns {@link FixedUCTokenProvider} if
   * a static token is provided, or {@link OAuthUCTokenProvider} if OAuth credentials are provided.
   *
   * @param options containing authentication configuration without prefix. Expected keys:
   *                <ul>
   *                  <li>{@code token} - for fixed token authentication, or</li>
   *                  <li>{@code oauth.uri}, {@code oauth.clientId}, {@code oauth.clientSecret} -
   *                      for OAuth 2.0 client authentication flow (all three required)</li>
   *                </ul>
   * @return a token provider instance
   * @throws IllegalArgumentException if options are missing or incomplete
   */
  static UCTokenProvider create(Map<String, String> options) {
    // If token is available, use FixedUCTokenProvider.
    String token = options.get(Constants.TOKEN);
    if (token != null) {
      return new FixedUCTokenProvider(token);
    }

    // If OAuth options is available, use OAuthUCTokenProvider.
    String oauthUri = options.get(Constants.OAUTH_URI);
    String oauthClientId = options.get(Constants.OAUTH_CLIENT_ID);
    String oauthClientSecret = options.get(Constants.OAUTH_CLIENT_SECRET);
    if (oauthUri != null || oauthClientId != null || oauthClientSecret != null) {
      Preconditions.checkArgument(oauthUri != null && oauthClientId != null && oauthClientSecret != null,
          "Incomplete OAuth configuration detected. All of the keys are required: " +
              "oauth.uri, oauth.clientId, oauth.clientSecret. Please ensure they are " +
              "all set.");

      return new OAuthUCTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
    }

    throw new IllegalArgumentException("Cannot determine UC authentication " +
            "configuration from options, please set %stoken for static token authentication or " +
            "oauth.uri, oauth.clientId, oauth.clientSecret for OAuth 2.0 authentication " +
            "(all three required)");
  }
}
