package io.unitycatalog.server.security;

/**
 * An identity provider whose access tokens the server accepts directly in {@code external}
 * authentication mode, configured as {@code server.authentication.idp.<name>.*}.
 *
 * @param name the IdP's name, the same as a SCIM principal's IdP name
 * @param issuer the exact {@code iss} of its tokens
 * @param audience the {@code aud} its tokens must contain
 * @param subjectClaim which claims name the principal
 */
public record IdentityProvider(
    String name, String issuer, String audience, SubjectClaim subjectClaim) {}
