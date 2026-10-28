package io.unitycatalog.spark.auth;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class AuthConfigUtils {
  private static final String AUTH_PREFIX = "auth.";
  private static final String TYPE = "type";
  private static final String STATIC_TYPE = "static";
  private static final String STATIC_TOKEN = "token";
  private static final String CATALOG_URI = "catalogUri";

  private AuthConfigUtils() {}

  public static Map<String, String> buildAuthConfigs(Map<String, String> configs) {
    // Spark lowercases option keys, so 'auth.catalogUri' arrives as 'cataloguri'. A
    // case-insensitive map keeps it and the 'catalogUri' added below from becoming two keys.
    Map<String, String> newConfigs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    for (Map.Entry<String, String> e : configs.entrySet()) {
      if (e.getKey().startsWith(AuthConfigUtils.AUTH_PREFIX)) {
        // Remove the 'auth.' prefix from the key and add the normalized key-value pair.
        String newKey = e.getKey().substring(AuthConfigUtils.AUTH_PREFIX.length()).trim();
        if (!newKey.isEmpty()) {
          newConfigs.put(newKey, e.getValue());
        }
      }
    }

    // Unity Catalog versions 0.3.0 and earlier did not use the 'auth.token' key. To maintain
    // backward compatibility, we also copy the legacy 'token' key directly into the new config map.
    // A present key (including an empty value) selects static auth. That is the supported way to
    // talk to a server with server.authorization=disable; omitting both token and auth.type is not.
    String token = configs.get(AuthConfigUtils.STATIC_TOKEN);
    if (token != null) {
      Preconditions.checkArgument(
          !newConfigs.containsKey(AuthConfigUtils.STATIC_TOKEN),
          "Static token was configured twice, choose only one: 'token' (legacy) or 'auth.token'"
              + " (new-style).");

      newConfigs.put(TYPE, STATIC_TYPE);
      newConfigs.put(STATIC_TOKEN, token);
    }

    // Token providers that call the UC server themselves (e.g. the OAuth token exchange) need the
    // catalog URI. It always comes from the catalog 'uri' option, replacing any 'auth.catalogUri',
    // so the exchange can only go to the catalog's server.
    newConfigs.remove(CATALOG_URI);
    String uri = configs.get(OptionsUtil.URI);
    if (uri != null) {
      newConfigs.put(CATALOG_URI, uri);
    }

    return new CaseInsensitiveStringMap(newConfigs);
  }

  /**
   * Returns the catalog options for Delta's catalog, which builds its own UC token provider from
   * the 'auth.' options alone. When there are 'auth.' options, 'auth.catalogUri' is set to the
   * catalog 'uri' so that provider can reach the server too. Otherwise the options are returned
   * unchanged: Delta then falls back to the legacy 'token' option, which a lone 'auth.catalogUri'
   * would hide.
   */
  public static CaseInsensitiveStringMap withAuthCatalogUri(CaseInsensitiveStringMap options) {
    String uri = options.get(OptionsUtil.URI);
    boolean hasAuthOptions = options.keySet().stream().anyMatch(k -> k.startsWith(AUTH_PREFIX));
    if (uri == null || !hasAuthOptions) {
      return options;
    }
    // Case-insensitive, so a user-set 'auth.catalogUri' (lowercased by Spark) is replaced rather
    // than left beside it.
    Map<String, String> delegateOptions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    delegateOptions.putAll(options);
    delegateOptions.put(AUTH_PREFIX + CATALOG_URI, uri);
    return new CaseInsensitiveStringMap(delegateOptions);
  }
}
