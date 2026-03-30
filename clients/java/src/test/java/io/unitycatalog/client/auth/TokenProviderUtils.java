package io.unitycatalog.client.auth;

import java.util.Map;

public class TokenProviderUtils {
  public static TokenProvider create(String token) {
    return TokenProvider.create(
        Map.of(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE_VALUE, AuthConfigs.STATIC_TOKEN, token));
  }

  public static TokenProvider create(
      String oauthUri, String oauthClientId, String oauthClientSecret) {
    return TokenProvider.create(
        Map.of(
            AuthConfigs.TYPE, AuthConfigs.OAUTH_TYPE_VALUE,
            AuthConfigs.OAUTH_URI, oauthUri,
            AuthConfigs.OAUTH_CLIENT_ID, oauthClientId,
            AuthConfigs.OAUTH_CLIENT_SECRET, oauthClientSecret));
  }
}
