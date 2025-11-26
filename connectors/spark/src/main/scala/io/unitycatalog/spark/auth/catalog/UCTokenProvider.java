package io.unitycatalog.spark.auth.catalog;

import static org.sparkproject.guava.base.Preconditions.checkArgument;

import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

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
    return create(options, "spark.sql.catalog.<catalogName>.");
  }

  /**
   * Creates a token provider from Hadoop configuration.
   *
   * <p>Expected configuration properties:
   * <ul>
   *   <li>{@code fs.unitycatalog.token} - for fixed token authentication, or</li>
   *   <li>{@code fs.unitycatalog.oauth.uri}, {@code fs.unitycatalog.oauth.clientId},
   *       {@code fs.unitycatalog.oauth.clientSecret} - for OAuth 2.0 client authentication
   *       flow (all three required)</li>
   * </ul>
   *
   * @param conf Hadoop configuration containing Unity Catalog authentication properties
   * @return a token provider instance
   * @throws IllegalArgumentException if configuration is missing or incomplete
   */
  static UCTokenProvider create(Configuration conf) {
    Map<String, String> options = conf.getPropsWithPrefix(UCHadoopConf.FS_UC_PREFIX);
    return create(options, UCHadoopConf.FS_UC_PREFIX);
  }

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
   * @param optionKeyPrefix the prefix to prepend to option keys when constructing full
   *                        configuration key names in error messages. For example,
   *                        {@code "spark.sql.catalog.<catalogName>."} or {@code "fs.unitycatalog."}
   * @return a token provider instance
   * @throws IllegalArgumentException if options are missing or incomplete
   */
  private static UCTokenProvider create(Map<String, String> options, String optionKeyPrefix) {
    // If token is available, use FixedUCTokenProvider.
    String token = options.get(OptionsUtil.TOKEN);
    if (token != null) {
      return new FixedUCTokenProvider(token);
    }

    // If OAuth options is available, use OAuthUCTokenProvider.
    String oauthUri = options.get(OptionsUtil.OAUTH_URI);
    String oauthClientId = options.get(OptionsUtil.OAUTH_CLIENT_ID);
    String oauthClientSecret = options.get(OptionsUtil.OAUTH_CLIENT_SECRET);
    if (oauthUri != null || oauthClientId != null || oauthClientSecret != null) {
      checkArgument(oauthUri != null && oauthClientId != null && oauthClientSecret != null,
          "Incomplete OAuth configuration detected. All of the keys are required: " +
              "%soauth.uri, %soauth.clientId, %soauth.clientSecret. Please ensure they are " +
              "all set.", optionKeyPrefix, optionKeyPrefix, optionKeyPrefix);

      return new OAuthUCTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
    }

    throw new IllegalArgumentException(String.format("Cannot determine UC authentication " +
            "configuration from options, please set %stoken for static token authentication or " +
            "%soauth.uri, %soauth.clientId, %soauth.clientSecret for OAuth 2.0 authentication " +
            "(all three required)",
        optionKeyPrefix, optionKeyPrefix, optionKeyPrefix, optionKeyPrefix));
  }
}
