package io.unitycatalog.client.auth;

import java.util.Map;

class FixedTokenProvider implements TokenProvider {
  private final String token;

  FixedTokenProvider(String token) {
    this.token = token;
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> getConfigs() {
    return Map.of(AuthProps.TOKEN, token);
  }
}
