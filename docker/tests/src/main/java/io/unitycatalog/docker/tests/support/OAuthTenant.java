package io.unitycatalog.docker.tests.support;

/** OAuth tenant settings derived from UC_OAUTH_TEAM and UC_OAUTH_REALM. */
public record OAuthTenant(String team, String realm, String teamIdOverride) {

  private static final String DEFAULT_TEAM = "dev";
  private static final String DEFAULT_REALM = "dev.celonis.cloud";

  public static OAuthTenant fromEnv() {
    String team = envOrDefault("UC_OAUTH_TEAM", DEFAULT_TEAM);
    String realm = envOrDefault("UC_OAUTH_REALM", DEFAULT_REALM);
    String teamId = System.getenv("UC_OAUTH_TEAM_ID");
    if (teamId != null && teamId.isBlank()) {
      teamId = null;
    }
    return new OAuthTenant(team, realm, teamId);
  }

  public String host() {
    String fromEnv = System.getenv("UC_OAUTH_HOST");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.trim();
    }
    return team + "." + realm;
  }

  public String issuer() {
    String fromEnv = System.getenv("UC_OAUTH_ISSUER");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.trim();
    }
    return "https://" + host();
  }

  public String httpBaseUrl() {
    String fromEnv = System.getenv("UC_OAUTH_BASE_URL");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.replaceAll("/$", "");
    }
    return "http://" + host() + ":9010";
  }

  public String teamId() {
    if (teamIdOverride != null) {
      return teamIdOverride.trim();
    }
    return CelonisOAuthTestConstants.OAUTH_TEAM_ID_FALLBACK;
  }

  public boolean resolvesLocally(String hostname) {
    return hostname.equals(realm) || hostname.endsWith("." + realm);
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return value.trim();
  }
}
