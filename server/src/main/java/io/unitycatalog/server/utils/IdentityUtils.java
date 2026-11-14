package io.unitycatalog.server.utils;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.security.JwtTokenType;
import io.unitycatalog.server.service.AuthDecorator;

public class IdentityUtils {
  public static String findPrincipalEmailAddress() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
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
   * the SCIM API. Its subject is the id of the IdP principal's user, not an email.
   */
  public static boolean isScimToken() {
    DecodedJWT decodedJWT = ServiceRequestContext.current().attr(AuthDecorator.DECODED_JWT_ATTR);
    return decodedJWT != null
        && JwtTokenType.SCIM
            .name()
            .equals(decodedJWT.getClaim(JwtClaim.TOKEN_TYPE.key()).asString());
  }
}
