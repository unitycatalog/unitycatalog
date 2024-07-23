package io.unitycatalog.server.security;

import lombok.Getter;

import java.util.Set;

import static io.unitycatalog.server.security.JwtClaim.SUBJECT;

/**
 * Valid UC token types.
 *
 */
public enum JwtTokenType {

    SERVICE(Set.of(SUBJECT)),
    ACCESS(Set.of(SUBJECT));

    @Getter private final Set<JwtClaim> requiredClaims;

    JwtTokenType(java.util.Set<JwtClaim> requiredClaims) {
        this.requiredClaims = requiredClaims;
    }

}
