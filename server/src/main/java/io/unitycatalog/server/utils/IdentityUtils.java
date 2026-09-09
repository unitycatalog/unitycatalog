package io.unitycatalog.server.utils;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import java.util.UUID;

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
   * The user id an on-behalf-of request (see {@link AuthDecorator}) should be evaluated as, or null
   * for a normal request authenticated with its own token. Checked ahead of {@link
   * #findPrincipalEmailAddress()} by every caller that resolves "the current principal" — a
   * recipient's read reaches the catalog this way, presenting no token of its own to look a subject
   * up from.
   */
  public static UUID findOnBehalfOfUserId() {
    return ServiceRequestContext.current().attr(AuthDecorator.ON_BEHALF_OF_USER_ID_ATTR);
  }
}
