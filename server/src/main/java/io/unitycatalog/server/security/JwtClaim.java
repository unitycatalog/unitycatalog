package io.unitycatalog.server.security;

/** Valid claims for UC access tokens. */
public enum JwtClaim {
  ISSUER("iss"),
  SUBJECT("sub"),
  EMAIL("email"),
  EXPIRATION("exp"),
  ISSUED_AT("iat"),
  JWT_ID("jti"),
  KEY_ID("kid"),
  // The identity provider a SCIM token is for.
  IDP("idp"),

  TOKEN_TYPE("type");

  private final String key;

  JwtClaim(String key) {
    this.key = key;
  }

  public String key() {
    return key;
  }
}
