package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AsciiString;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication decorator that extracts user identity from trusted request headers.
 *
 * <p>This decorator is intended for deployments where a trusted reverse proxy (e.g. Envoy) sits in
 * front of the UC server, validates JWTs, and forwards verified claims as request headers. The
 * server trusts the proxy and reads the user's email from the configured header instead of
 * performing its own JWT validation.
 *
 * <p><b>Security:</b> When using this decorator, the UC server must NOT be directly accessible to
 * clients. Only the trusted proxy should be able to reach the server.
 */
public class TrustedHeaderAuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrustedHeaderAuthDecorator.class);

  private final AsciiString emailHeader;
  private final UserRepository userRepository;

  public TrustedHeaderAuthDecorator(String emailHeaderName, Repositories repositories) {
    this.emailHeader = AsciiString.of(emailHeaderName);
    this.userRepository = repositories.getUserRepository();
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("TrustedHeaderAuthDecorator checking {}", req.path());

    String email = req.headers().get(emailHeader);
    if (email == null || email.isBlank()) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED, "No identity header '" + emailHeader + "' found in request.");
    }

    User user;
    try {
      user = userRepository.getUserByEmail(email);
    } catch (Exception e) {
      LOGGER.debug("User not found: {}", email);
      user = null;
    }
    if (user == null || user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(ErrorCode.PERMISSION_DENIED, "User not allowed: " + email);
    }

    LOGGER.debug("Access allowed for subject: {}", email);

    ctx.setAttr(AuthDecorator.PRINCIPAL_EMAIL_ATTR, email);

    return delegate.serve(ctx, req);
  }
}
