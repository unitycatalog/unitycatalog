package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;

/**
 * Interface for providing access tokens to authenticate with Unity Catalog.
 *
 * <p>Implementations include:
 *
 * <ul>
 *   <li>{@link StaticTokenProvider} - uses a pre-configured static token
 *   <li>{@link OAuthTokenProvider} - obtains tokens via OAuth 2.0 client credentials flow
 * </ul>
 */
public interface TokenProvider {

  /**
   * Initializes the token provider with configuration parameters.
   *
   * @param configs configuration map with authentication settings
   * @throws IllegalArgumentException if required parameters are missing or invalid
   */
  void initialize(Map<String, String> configs);

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
   * configs.put("type", "my-access-token");
   * configs.put("token", "my-access-token");
   * TokenProvider provider = TokenProvider.createFromConfigs(configs);
   * }</pre>
   *
   * <p>Example usage with OAuth 2.0:
   *
   * <pre>{@code
   * Map<String, String> configs = new HashMap<>();
   * configs.put("type", "oauth");
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
  static TokenProvider create(Map<String, String> configs) {
    String authType = configs.get(AuthConfigs.TYPE);
    Preconditions.checkArgument(
        authType != null && !authType.trim().isEmpty(),
        "Value of '%s' to instantiate TokenProvider cannot be null or empty",
        AuthConfigs.TYPE);
    switch (authType) {
      case AuthConfigs.STATIC_TYPE:
        StaticTokenProvider staticTokenProvider = new StaticTokenProvider();
        staticTokenProvider.initialize(configs);
        return staticTokenProvider;

      case AuthConfigs.OAUTH_TYPE:
        OAuthTokenProvider oauthTokenProvider = new OAuthTokenProvider();
        oauthTokenProvider.initialize(configs);
        return oauthTokenProvider;

      default:
        try {
          TokenProvider customizedTokenProvider =
              (TokenProvider) Class.forName(authType).getDeclaredConstructor().newInstance();
          customizedTokenProvider.initialize(configs);
          return customizedTokenProvider;
        } catch (Exception e) {
          throw new RuntimeException(
              "Cannot determine unity catalog authentication configuration from configs, please "
                  + "ensure to use static token authentication, OAuth 2.0 authentication, or "
                  + "customized TokenProvider.",
              e);
        }
    }
  }
}
