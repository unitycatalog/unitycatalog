package io.unitycatalog.server.sharing;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.opensharing.principal.Caller;
import io.opensharing.runtime.ProviderIdentityResolver;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps a provider-admin HTTP request on OpenSharing to the UC-authenticated principal behind it.
 */
public final class UnityCatalogProviderIdentityResolver implements ProviderIdentityResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnityCatalogProviderIdentityResolver.class);
  private static final String BEARER_PREFIX = "Bearer ";

  private final ServerProperties serverProperties;
  private final UserRepository userRepository;
  private final JwksOperations jwksOperations;

  public UnityCatalogProviderIdentityResolver(
      ServerProperties serverProperties,
      SecurityContext securityContext,
      Repositories repositories) {
    this.serverProperties = serverProperties;
    this.userRepository = repositories.getUserRepository();
    this.jwksOperations = new JwksOperations(securityContext);
  }

  @Override
  public Optional<Caller> resolve(HttpServletRequest request) {
    String token = bearerToken(request);
    if (token == null) {
      return Optional.empty();
    }
    if (!serverProperties.isAuthorizationEnabled()) {
      return Optional.of(
          new Caller("uc-local", serverProperties.getOpenSharingPrincipalName(), token));
    }
    try {
      DecodedJWT decoded = JWT.decode(token);
      if (!INTERNAL.equals(decoded.getIssuer())) {
        return Optional.empty();
      }
      JWTVerifier verifier =
          jwksOperations.verifierForIssuerAndKey(
              decoded.getIssuer(), decoded.getKeyId(), decoded.getAlgorithm());
      decoded = verifier.verify(decoded);
      User user = userRepository.getUserByEmail(decoded.getSubject());
      if (user == null || user.getState() != User.StateEnum.ENABLED) {
        return Optional.empty();
      }
      return Optional.of(new Caller(user.getId(), user.getEmail(), token));
    } catch (Exception e) {
      LOGGER.debug("Could not resolve OpenSharing provider identity from UC token", e);
      return Optional.empty();
    }
  }

  private static String bearerToken(HttpServletRequest request) {
    String authorization = request.getHeader("Authorization");
    if (authorization != null && authorization.startsWith(BEARER_PREFIX)) {
      return authorization.substring(BEARER_PREFIX.length()).trim();
    }
    return null;
  }
}
