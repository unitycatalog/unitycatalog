package io.unitycatalog.server.persist.model;

/**
 * The identity provider identity a user row is linked to: the token issuer, the claim that names
 * the principal (such as {@code sub}, or {@code uid} and {@code cid} on Okta), and its value. The
 * claim is kept so that a person's id and a client id with the same value are never confused.
 */
public record UserLink(String issuer, String claim, String value) {}
