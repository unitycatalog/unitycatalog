package io.unitycatalog.client.auth;

class AuthProps {
  private AuthProps() {}

  static final String AUTH_TYPE = "auth.type";

  // Configure keys for static token provider.
  static final String STATIC_AUTH_TYPE = "static";
  static final String STATIC_TOKEN = "token";

  // Configure keys for oauth token provider.
  static final String OAUTH_AUTH_TYPE = "oauth";
  static final String OAUTH_URI = "oauth.uri";
  static final String OAUTH_CLIENT_ID = "oauth.clientId";
  static final String OAUTH_CLIENT_SECRET = "oauth.clientSecret";
}
