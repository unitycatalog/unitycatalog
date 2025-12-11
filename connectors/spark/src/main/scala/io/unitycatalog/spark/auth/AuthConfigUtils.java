package io.unitycatalog.spark.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class AuthConfigUtils {
  private static final String AUTH_PREFIX = "auth.";
  private static final String TYPE = "type";
  private static final String STATIC_TYPE = "static";
  private static final String STATIC_TOKEN = "token";

  private AuthConfigUtils() {
  }

  public static Map<String, String> buildAuthConfigs(Map<String, String> configs) {
    Map<String, String> newConfigs = new HashMap<>();

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
    String token = configs.get(AuthConfigUtils.STATIC_TOKEN);
    if (token != null) {
      Preconditions.checkArgument(!newConfigs.containsKey(AuthConfigUtils.STATIC_TOKEN),
          "Static token was configured twice, choose only one: 'token' (legacy) or 'auth.token' (new-style).");

      newConfigs.put(TYPE, STATIC_TYPE);
      newConfigs.put(STATIC_TOKEN, token);
    }

    return new CaseInsensitiveStringMap(newConfigs);
  }
}
