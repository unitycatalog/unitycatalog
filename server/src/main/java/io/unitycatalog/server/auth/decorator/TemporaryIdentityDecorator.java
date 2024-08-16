package io.unitycatalog.server.auth.decorator;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AttributeKey;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryIdentityDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryIdentityDecorator.class);

  public static final AttributeKey<DecodedJWT> DECODED_JWT_ATTR =
      AttributeKey.valueOf(DecodedJWT.class, "DECODED_JWT_ATTR");

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {

    String authorization =
        req.headers().stream()
            .filter(h -> h.getKey().equals(HttpHeaderNames.AUTHORIZATION))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);

    LOGGER.info("Authorization: {}", authorization);

    String subject = null;

    if (authorization != null) {
      String[] parts = authorization.split(" ", 2);
      if (parts.length == 2 && parts[0].equalsIgnoreCase("bearer")) {
        subject = parts[1];
      } else if (parts.length == 1) {
        subject = parts[0];
      } else {
        LOGGER.warn("Didn't understand Authorization header.");
      }
    } else {
      LOGGER.warn("No Authorization header.");
    }

    String jwt = JWT.create().withClaim("sub", subject).sign(Algorithm.none());
    DecodedJWT decodedJWT = JWT.decode(jwt);

    ctx.setAttr(DECODED_JWT_ATTR, decodedJWT);

    return delegate.serve(ctx, req);
  }
}
