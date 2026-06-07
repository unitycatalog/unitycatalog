package io.unitycatalog.server.utils;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import io.unitycatalog.server.service.TrustedHeaderAuthDecorator;

public class IdentityUtils {
  public static String findPrincipalEmailAddress() {
    ServiceRequestContext ctx = ServiceRequestContext.current();

    // When deployed behind a trusted proxy, identity comes from a trusted header rather than a JWT.
    String headerPrincipal = ctx.attr(TrustedHeaderAuthDecorator.PRINCIPAL_ATTR);
    if (headerPrincipal != null) {
      return headerPrincipal;
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
}
