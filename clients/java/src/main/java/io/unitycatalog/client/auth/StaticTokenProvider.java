package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;

class StaticTokenProvider implements TokenProvider {
  private String token;

  @Override
  public void initialize(Map<String, String> configs) {
    String tokenStr = configs.get(AuthConfigs.STATIC_TOKEN);
    Preconditions.checkArgument(
        tokenStr != null,
        "Invalid Unity Catalog authentication configuration: token cannot be null ");
    this.token = tokenStr;
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> getConfigs() {
    return Map.of(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE, AuthConfigs.STATIC_TOKEN, token);
  }
}
