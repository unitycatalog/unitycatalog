package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;

class StaticTokenProvider implements TokenProvider {
  private String token;

  @Override
  public void initialize(Map<String, String> configs) {
    String token = configs.get(AuthConfigs.STATIC_TOKEN);
    Preconditions.checkArgument(
        token != null, "Configuration key '%s' is missing or empty", AuthConfigs.STATIC_TOKEN);
    this.token = token;
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> configs() {
    return Map.of(AuthConfigs.TYPE, AuthConfigs.STATIC_TYPE, AuthConfigs.STATIC_TOKEN, token);
  }
}
