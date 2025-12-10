package io.unitycatalog.client.auth;

/** Internal class - not intended for direct use. */
class AuthConfigs {
  private AuthConfigs() {}

  // Define the authentication type, where the type can be:
  // 1. static: which uses the StaticTokenProvider to generate the static token.
  // 2. oauth: which uses the OAuthTokenProvider to generate the oauth token.
  // 3. fully qualified class name of the custom TokenProvider implementation.
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
