package io.unitycatalog.server.security;

import static io.unitycatalog.server.security.JwtClaim.SUBJECT;

import java.util.Set;
import lombok.Getter;

/** Valid UC token types. */
public enum JwtTokenType {
  SERVICE(Set.of(SUBJECT)),
  ACCESS(Set.of(SUBJECT));

  @Getter private final Set<JwtClaim> requiredClaims;

  JwtTokenType(java.util.Set<JwtClaim> requiredClaims) {
    this.requiredClaims = requiredClaims;
  }
}
