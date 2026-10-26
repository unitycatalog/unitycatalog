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
    // case-insensitive
    // map keeps it and the 'catalogUri' added below from becoming two keys.
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
    // catalog URI, and the exchange sends the OAuth token to it, so the catalog 'uri' option is its
    // only source. 'auth.catalogUri' is still accepted when it names the same URI: Delta's UC
    // commit
    // coordinator builds its token provider from the 'auth.' options alone, so that is how it gets
    // the catalog URI.
    String uri = configs.get(OptionsUtil.URI);
    String authCatalogUri = newConfigs.remove(CATALOG_URI);
    if (authCatalogUri != null) {
      Preconditions.checkArgument(
          uri != null && withoutTrailingSlashes(authCatalogUri).equals(withoutTrailingSlashes(uri)),
          "'%s%s' (%s) must match the catalog '%s' option (%s).",
          AUTH_PREFIX,
          CATALOG_URI,
          authCatalogUri,
          OptionsUtil.URI,
          uri);
    }
    if (uri != null) {
      newConfigs.put(CATALOG_URI, uri);
    }

    return new CaseInsensitiveStringMap(newConfigs);
  }

  private static String withoutTrailingSlashes(String uri) {
    return uri.replaceAll("/+$", "");
  }
}
