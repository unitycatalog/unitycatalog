package io.unitycatalog.server.security;

import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.server.persist.model.UserLink;
import java.util.List;

/** Which claims of an identity provider's access token name the person or machine. */
public enum SubjectClaim {
  /** OIDC's {@code sub}, for people and machines. */
  SUB("sub"),
  /**
   * Okta's reserved {@code uid} for a person, else {@code cid} for a machine. A person's token also
   * carries {@code cid}, so {@code uid} decides. Okta's {@code sub} is an editable mapping and is
   * never read.
   */
  OKTA("uid", "cid");

  private final List<String> claims;

  SubjectClaim(String... claims) {
    this.claims = List.of(claims);
  }

  /** Returns the link for a verified token from {@code issuer}, or null if the claim is missing. */
  public UserLink linkOf(String issuer, DecodedJWT jwt) {
    for (String claim : claims) {
      String value = jwt.getClaim(claim).asString();
      if (value != null && !value.isEmpty()) {
        return new UserLink(issuer, claim, value);
      }
    }
    return null;
  }

  /** The claims read, for a refusal message, such as {@code uid or cid}. */
  public String claimNames() {
    return String.join(" or ", claims);
  }
}
