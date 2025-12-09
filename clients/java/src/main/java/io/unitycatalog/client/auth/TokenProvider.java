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
   * <p>The returned configuration map can be passed to {@link #create(Map)} to create a new token
   * provider instance with the same configuration:
   *
   * <pre>{@code
   * TokenProvider existingProvider = ...;
   * TokenProvider newProvider = TokenProvider.create(existingProvider.configs());
   * }</pre>
   *
   * @return a map of configuration key-value pairs
   */
  Map<String, String> configs();

  /**
   * Creates a token provider from a configuration map.
   *
   * <p>This method creates a token provider based on the required {@code "type"} configuration
   * parameter. It supports three authentication modes:
   *
   * <ul>
   *   <li><b>Static token authentication:</b> Set {@code "type"} to {@code "static"} and provide a
   *       {@code "token"} key with the access token value
   *   <li><b>OAuth 2.0 authentication:</b> Set {@code "type"} to {@code "oauth"} and provide all
   *       three OAuth keys: {@code "oauth.uri"}, {@code "oauth.clientId"}, and {@code
   *       "oauth.clientSecret"}
   *   <li><b>Custom authentication:</b> Set {@code "type"} to the fully qualified class name of a
   *       custom {@link TokenProvider} implementation
   * </ul>
   *
   * <p>Example usage with static token:
   *
   * <pre>{@code
   * Map<String, String> configs = new HashMap<>();
   * configs.put("type", "static");
   * configs.put("token", "my-access-token");
   * TokenProvider provider = TokenProvider.create(configs);
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
   * TokenProvider provider = TokenProvider.create(configs);
   * }</pre>
   *
   * <p>Example usage with custom token provider:
   *
   * <pre>{@code
   * Map<String, String> configs = new HashMap<>();
   * configs.put("type", "com.example.MyCustomTokenProvider");
   * // Add any additional configs required by your custom provider
   * TokenProvider provider = TokenProvider.create(configs);
   * }</pre>
   *
   * <p>You can also use {@link #configs()} to obtain the configuration from an existing token
   * provider and pass it to this method to create a new instance:
   *
   * <pre>{@code
   * TokenProvider existingProvider = ...;
   * TokenProvider newProvider = TokenProvider.create(existingProvider.getConfigs());
   * }</pre>
   *
   * @param configs a map containing authentication configuration parameters. Must contain a
   *     non-null, non-empty {@code "type"} key.
   * @return a token provider configured according to the provided parameters
   * @throws IllegalArgumentException if the {@code "type"} parameter is null, empty, or if required
   *     authentication parameters for the specified type are missing or invalid
   * @throws RuntimeException if a custom TokenProvider class cannot be instantiated (e.g., class
   *     not found, no default constructor, or instantiation failure)
   */
  static TokenProvider create(Map<String, String> configs) {
    String authType = configs.get(AuthConfigs.TYPE);
    Preconditions.checkArgument(
        authType != null && !authType.trim().isEmpty(),
        "Required configuration key '%s' is missing or empty. "
            + "Must be 'static', 'oauth', or a fully qualified TokenProvider class name.",
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
          TokenProvider customTokenProvider =
              (TokenProvider) Class.forName(authType).getDeclaredConstructor().newInstance();
          customTokenProvider.initialize(configs);
          return customTokenProvider;
        } catch (Exception e) {
          throw new RuntimeException(
              String.format(
                  "Failed to instantiate custom TokenProvider '%s'. Ensure the class exists, "
                      + "implements TokenProvider, and has a public no-arg constructor.",
                  authType),
              e);
        }
    }
  }
}
