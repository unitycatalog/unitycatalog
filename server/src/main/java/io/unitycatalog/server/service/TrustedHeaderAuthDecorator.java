package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AttributeKey;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.utils.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trusted-header authentication decorator.
 *
 * <p>This decorator is intended for deployments where Unity Catalog sits behind a fronting proxy
 * (e.g. Envoy) that terminates authentication against the identity provider. Rather than validating
 * a JWT, it trusts the principal email injected by the proxy in a configured header.
 *
 * <p>The header value is looked up against the user database and the request is only allowed if it
 * resolves to an existing, {@link User.StateEnum#ENABLED ENABLED} user. The resolved principal
 * email is added to the request attributes so downstream authorization can reference it (see {@code
 * IdentityUtils}).
 *
 * <p>SECURITY: This mode trusts the header unconditionally. It is only safe when clients cannot
 * reach Unity Catalog directly and the proxy strips/overwrites the header on inbound requests.
 */
public class TrustedHeaderAuthDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrustedHeaderAuthDecorator.class);

  public static final AttributeKey<String> PRINCIPAL_ATTR =
      AttributeKey.valueOf(String.class, "TRUSTED_HEADER_PRINCIPAL_ATTR");

  private final String headerName;
  private final UserRepository userRepository;

  public TrustedHeaderAuthDecorator(ServerProperties serverProperties, Repositories repositories) {
    this.headerName = serverProperties.getTrustedHeaderName();
    this.userRepository = repositories.getUserRepository();
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("TrustedHeaderAuthDecorator checking {}", req.path());

    String principal = req.headers().get(headerName);
    if (principal == null || principal.trim().isEmpty()) {
      throw new AuthorizationException(
          ErrorCode.UNAUTHENTICATED, "No trusted identity header found.");
    }

    User user;
    try {
      user = userRepository.getUserByEmail(principal);
    } catch (Exception e) {
      LOGGER.debug("User not found: {}", principal);
      user = null;
    }
    if (user == null || user.getState() != User.StateEnum.ENABLED) {
      throw new AuthorizationException(
          ErrorCode.PERMISSION_DENIED, "User not allowed: " + principal);
    }

    LOGGER.debug("Access allowed for subject: {}", principal);

    ctx.setAttr(PRINCIPAL_ATTR, principal);

    return delegate.serve(ctx, req);
  }
}
