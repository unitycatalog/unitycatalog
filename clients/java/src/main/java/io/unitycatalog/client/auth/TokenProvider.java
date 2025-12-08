package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.Preconditions;
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
   * Returns the configuration associated with this token provider.
   *
   * @return a map of configuration key-value pairs
   */
  Map<String, String> getConfigs();

  /**
   * Creates a token provider using a static access token.
   *
   * <p>Use this method when you have a pre-configured access token that doesn't change. The
   * returned provider will always return the same token for authentication.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * TokenProvider provider = TokenProvider.create("my-access-token");
   * // returns "my-access-token"
   * String token = provider.accessToken();
   * }</pre>
   *
   * @param token the access token to use for authentication
   * @return a token provider that returns the specified token
   * @throws NullPointerException if token is null
   */
  static TokenProvider create(String token) {
    return new FixedTokenProvider(token);
  }

  /**
   * Creates a token provider using OAuth 2.0 client credentials flow.
   *
   * <p>Use this method to automatically obtain and refresh access tokens using the OAuth 2.0 client
   * credentials grant type. The returned provider will handle token expiration and renewal
   * automatically.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * TokenProvider provider = TokenProvider.createFromOAuthConfigs(
   *     "https://auth.example.com/token",
   *     "my-client-id",
   *     "my-client-secret"
   * );
   * String token = provider.accessToken(); // obtains and returns a valid token
   * }</pre>
   *
   * @param oauthUri the OAuth 2.0 token endpoint URI (e.g., "https://uc.example.com/token")
   * @param oauthClientId the OAuth 2.0 client identifier issued by the authorization server
   * @param oauthClientSecret the OAuth 2.0 client secret issued by the authorization server
   * @return a token provider that obtains tokens via OAuth 2.0
   * @throws NullPointerException if any parameter is null
   */
  static TokenProvider createFromOAuthConfigs(
      String oauthUri, String oauthClientId, String oauthClientSecret) {
    return new OAuthTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
  }

  /**
   * Creates a token provider from a configuration map.
   *
   * <p>This method automatically determines which type of authentication to use based on the
   * configuration parameters provided. It supports two authentication modes:
   *
   * <ul>
   *   <li><b>Static token authentication:</b> Provide a {@code "token"} key
   *   <li><b>OAuth 2.0 authentication:</b> Provide all three OAuth keys: {@code "oauth.uri"},
   *       {@code "oauth.clientId"}, and {@code "oauth.clientSecret"}
   * </ul>
   *
   * <p>Example usage with static token:
   *
   * <pre>{@code
   * Map<String, String> configs = new HashMap<>();
   * configs.put("token", "my-access-token");
   * TokenProvider provider = TokenProvider.createFromConfigs(configs);
   * }</pre>
   *
   * <p>Example usage with OAuth 2.0:
   *
   * <pre>{@code
   * Map<String, String> configs = new HashMap<>();
   * configs.put("oauth.uri", "https://uc.example.com/token");
   * configs.put("oauth.clientId", "my-client-id");
   * configs.put("oauth.clientSecret", "my-client-secret");
   * TokenProvider provider = TokenProvider.createFromConfigs(configs);
   * }</pre>
   *
   * @param configs a map containing authentication configuration parameters
   * @return a token provider configured according to the provided parameters
   * @throws IllegalArgumentException if both token and OAuth configs are provided, if OAuth configs
   *     are incomplete (only some OAuth parameters are set), or if no valid authentication
   *     configuration is found
   * @throws NullPointerException if configs is null
   */
  static TokenProvider createFromConfigs(Map<String, String> configs) {
    String token = configs.get(AuthProps.TOKEN);
    String oauthUri = configs.get(AuthProps.OAUTH_URI);
    String oauthClientId = configs.get(AuthProps.OAUTH_CLIENT_ID);
    String oauthClientSecret = configs.get(AuthProps.OAUTH_CLIENT_SECRET);

    if (token != null) {
      Preconditions.checkArgument(
          oauthUri == null && oauthClientId == null && oauthClientSecret == null,
          "Invalid Unity Catalog authentication configuration: token-based and OAuth "
              + "settings were both supplied. Configure exactly one authentication method.");
      return create(token);
    }

    if (oauthUri != null || oauthClientId != null || oauthClientSecret != null) {
      Preconditions.checkArgument(
          oauthUri != null && oauthClientId != null && oauthClientSecret != null,
          "Incomplete OAuth configuration detected. All of the keys are required: "
              + "oauthUri, oauthClientId, oauthClientSecret. Please ensure they are "
              + "all set.");

      return createFromOAuthConfigs(oauthUri, oauthClientId, oauthClientSecret);
    }

    throw new IllegalArgumentException(
        "Cannot determine unity catalog authentication "
            + "configuration from options, please set token for static token authentication or "
            + "oauth.uri, oauth.clientId, oauth.clientSecret for OAuth 2.0 authentication "
            + "(all three required)");
  }
}
