package io.unitycatalog.server.utils;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.service.AuthDecorator;

public class IdentityUtils {
  /** The owner of the users and groups the admin creates; other rows name their IdP. */
  public static final String ADMIN_OWNER = "admin";

  public static String findPrincipalEmailAddress() {
    return findPrincipalEmailAddress(ServiceRequestContext.current());
  }

  /**
   * The caller's email, or for a SCIM token its principal's user id; null without a token. An IdP
   * token's {@code sub} is not an email, so {@code AuthDecorator} records its user's email.
   */
  public static String findPrincipalEmailAddress(ServiceRequestContext ctx) {
    String linkedUserEmail = ctx.attr(AuthDecorator.PRINCIPAL_EMAIL_ATTR);
    if (linkedUserEmail != null) {
      return linkedUserEmail;
    }
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    // TODO: if/when authorization becomes mandatory, maybe just throw an exception here?
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return sub.asString();
    } else {
      return null;
    }
  }

  /**
   * Whether the request carries an IdP's SCIM token, which {@code AuthDecorator} accepts only on
   * the SCIM API. Its subject is the id of the IdP principal's user, not an email. Only this server
   * issues SCIM tokens, so an IdP token's own {@code type} claim never counts.
   */
  public static boolean isScimToken() {
    DecodedJWT decodedJWT = ServiceRequestContext.current().attr(AuthDecorator.DECODED_JWT_ATTR);
    return decodedJWT != null
        && INTERNAL.equals(decodedJWT.getIssuer())
        && JwtTokenType.SCIM
            .name()
            .equals(decodedJWT.getClaim(JwtClaim.TOKEN_TYPE.key()).asString());
  }

  /**
   * The owner of the users and groups this request writes: the IdP that a SCIM token names, else
   * {@link #ADMIN_OWNER}.
   */
  public static String callerOwner() {
    // Outside a request, such as a direct call from server code, there is no token.
    ServiceRequestContext ctx = ServiceRequestContext.currentOrNull();
    if (ctx == null || !isScimToken()) {
      return ADMIN_OWNER;
    }
    String idp = ctx.attr(AuthDecorator.DECODED_JWT_ATTR).getClaim(JwtClaim.IDP.key()).asString();
    // bin/uc-admin-token always sets it; without it the token could pass for the admin's.
    if (idp == null || idp.equals(ADMIN_OWNER)) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "The SCIM token does not name a valid IdP.");
    }
    return idp;
  }
}
