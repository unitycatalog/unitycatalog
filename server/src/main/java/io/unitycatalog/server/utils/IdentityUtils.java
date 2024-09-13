package io.unitycatalog.server.utils;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import java.util.UUID;

public class IdentityUtils {

  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

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

  public static UUID findPrincipalId() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    // TODO: if/when authorization becomes mandatory, maybe just throw an exception here?
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return UUID.fromString(USER_REPOSITORY.getUserByEmail(sub.asString()).getId());
    } else {
      return null;
    }
  }
}
