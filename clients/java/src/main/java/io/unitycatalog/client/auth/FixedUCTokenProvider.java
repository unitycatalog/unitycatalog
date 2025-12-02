package io.unitycatalog.client.auth;

import java.util.Map;

public class FixedUCTokenProvider implements UCTokenProvider {
  private final String token;

  public FixedUCTokenProvider(String token) {
    this.token = token;
  }

  public static FixedUCTokenProvider create(String token) {
    return new FixedUCTokenProvider(token);
  }

  @Override
  public String accessToken() {
    return token;
  }

  @Override
  public Map<String, String> properties() {
    return Map.of();
  }
}
