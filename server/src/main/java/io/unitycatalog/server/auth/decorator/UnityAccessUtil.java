package io.unitycatalog.server.auth.decorator;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.UUID;

public class UnityAccessUtil {
  public static UUID findPrincipalId() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
    DecodedJWT decodedJWT = ctx.attr(TemporaryIdentityDecorator.DECODED_JWT_ATTR);
    Claim sub = decodedJWT.getClaim("sub");
    return UUID.fromString(sub.asString());
  }
}
