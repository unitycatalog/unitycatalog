package io.unitycatalog.server.auth.decorator;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.persist.UserRepository;
import java.util.UUID;

public class UnityAccessUtil {

  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

  public static UUID findPrincipalId() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
    DecodedJWT decodedJWT = ctx.attr(TemporaryIdentityDecorator.DECODED_JWT_ATTR);
    Claim sub = decodedJWT.getClaim("sub");

    return UUID.fromString(USER_REPOSITORY.getUserByEmail(sub.asString()).getId());
  }
}
