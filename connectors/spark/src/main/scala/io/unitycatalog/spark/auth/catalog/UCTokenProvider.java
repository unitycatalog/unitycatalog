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
   * a token is provided, or {@link OAuthUCTokenProvider} if OAuth credentials are provided.
   *
   * @param options containing authentication credentials
   * @return a token provider instance
   * @throws IllegalArgumentException if options are missing or incomplete
   */
  static UCTokenProvider create(Map<String, String> options) {
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
              "spark.sql.catalog.<catalogName>.oauthUri, " +
              "spark.sql.catalog.<catalogName>.oauthClientId, " +
              "spark.sql.catalog.<catalogName>.oauthClientSecret. " +
              "Please ensure they are all set.");

      return new OAuthUCTokenProvider(oauthUri, oauthClientId, oauthClientSecret);
    }

    throw new IllegalArgumentException("Cannot determine UCTokenProvider from options");
  }

  /**
   * Creates a token provider from Hadoop configuration by extracting Unity Catalog properties
   * and delegating to {@link #create(Map)}.
   *
   * @param conf Hadoop configuration containing Unity Catalog properties
   * @return a token provider instance
   * @throws IllegalArgumentException if configuration is missing or incomplete
   */
  static UCTokenProvider create(Configuration conf) {
    Map<String, String> options = conf.getPropsWithPrefix(UCHadoopConf.FS_UC_PREFIX);
    return create(options);
  }
}
