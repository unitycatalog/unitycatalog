package io.unitycatalog.server.utils;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;

public class IdentityUtils {
  public static String findPrincipalEmailAddress() {
    ServiceRequestContext ctx = ServiceRequestContext.current();

    // First try to get user from DAPI token authentication
    User user = ctx.attr(AuthDecorator.PRINCIPAL_ATTR);
    if (user != null) {
      return user.getEmail();
    }

    // Fall back to JWT authentication
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    // TODO: if/when authorization becomes mandatory, maybe just throw an exception here?
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return sub.asString();
    } else {
      return null;
    }
  }

  public static String userId(ServiceRequestContext ctx) {
    // First try to get user from DAPI token authentication
    User user = ctx.attr(AuthDecorator.PRINCIPAL_ATTR);
    if (user != null) {
      return user.getEmail();
    }

    // Fall back to JWT authentication
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return sub.asString();
    }

    return null;
  }
}
