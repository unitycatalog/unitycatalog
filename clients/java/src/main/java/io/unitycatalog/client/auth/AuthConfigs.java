package io.unitycatalog.client.auth;

class AuthConfigs {
  private AuthConfigs() {}

  // Define the authentication type, where the type can be:
  // 1. {@link STATIC_TYPE}: which use the {@link StaticTokenProvider} to generate the static token.
  // 2. {@link OAUTH_TYPE}: which use the {@link OAuthTokenProvider} to generate to oauth token.
  // 3. customized TokenProvider implementation.
  static final String TYPE = "type";

  // Configure keys for static token provider.
  static final String STATIC_TYPE = "static";
  static final String STATIC_TOKEN = "token";

  // Configure keys for oauth token provider.
  static final String OAUTH_TYPE = "oauth";
  static final String OAUTH_URI = "oauth.uri";
  static final String OAUTH_CLIENT_ID = "oauth.clientId";
  static final String OAUTH_CLIENT_SECRET = "oauth.clientSecret";
}
