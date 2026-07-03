package io.unitycatalog.docker.tests.support;

public final class CelonisOAuthTestConstants {

  public static final String OAUTH_CLIENT_ID = "unity-catalog-local";
  public static final String OAUTH_CLIENT_SECRET = "unity-catalog-local-secret";
  /** Fallback only; prefer {@link #oauthTeamId()} from bootstrapper-seeded team DB. */
  public static final String OAUTH_TEAM_ID_FALLBACK = "79257834-828d-48cb-951d-75294d6e1cce";
  public static final String OAUTH_TEAM_DOMAIN = "dev";
  public static final String OAUTH_INTERNAL_JWT_SECRET = "my-super-secret-key";
  public static final String OAUTH_REDIRECT_URI = "http://127.0.0.1:8080/callback";
  public static final String OAUTH_INTERNAL_SERVICE_NAME = "unity-catalog-tests";

  private CelonisOAuthTestConstants() {}

  public static String oauthBaseUrl() {
    String fromEnv = System.getenv("UC_OAUTH_BASE_URL");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.replaceAll("/$", "");
    }
    return "http://dev.dev.celonis.cloud:9010";
  }

  public static String oauthTeamId() {
    String fromEnv = System.getenv("UC_OAUTH_TEAM_ID");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.trim();
    }
    return OAUTH_TEAM_ID_FALLBACK;
  }
}
