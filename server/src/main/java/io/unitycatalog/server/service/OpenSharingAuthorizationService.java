package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lets an embedded or standalone OpenSharing instance ask, in one call: whose token is this, and
 * are they allowed to do the thing they are asking to do. Every provider-admin request OpenSharing
 * serves is authenticated this way instead of OpenSharing keeping any credential of its own — a
 * caller's PAT authenticates them to UC exactly once, on the request it actually concerns, and
 * OpenSharing stores only the {@code user_id} this returns, never the token.
 *
 * <p>Not a new authentication scheme: the {@code Authorization} header is the same bearer token
 * every other Unity Catalog endpoint reads, so when authorization is enabled this runs through the
 * same {@code AuthDecorator} as everything else under {@code basePath} and this class only reads
 * what it already verified ({@link UserRepository#findPrincipalId()}). When authorization is
 * disabled, no decorator runs for any endpoint — this one included — so a caller is identified
 * leniently, from the token's own (unverified) subject or, failing that, the token text itself,
 * matching how every other disabled-mode code path in this server already behaves: no real security
 * in this mode, permissive by design, not a special case invented for OpenSharing.
 */
public class OpenSharingAuthorizationService extends AuthorizedService
    implements UnityCatalogRestService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OpenSharingAuthorizationService.class);
  private static final String BEARER_PREFIX = "Bearer ";

  private final MetastoreRepository metastoreRepository;

  public OpenSharingAuthorizationService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  /**
   * @param request the incoming request, read directly only when authorization is disabled (see
   *     {@link #leniantIdentity})
   * @param privilege e.g. {@code CREATE_SHARE} — optional; omit it to resolve identity only, with
   *     no permission check (the caller is always "authorized" in that case, since simply
   *     presenting a valid token is all that was asked)
   */
  // A GET, not a POST: this reads and reports, changing nothing -- checking a privilege has no
  // side effect any more than checking whose token this is does.
  //
  // #principal != null lets UnityAccessDecorator's own bookkeeping know this endpoint is
  // deliberately open to any authenticated caller: the privilege named in the "privilege" query
  // parameter is dynamic, decided at request time, so it cannot be a fixed SpEL expression the
  // decorator itself evaluates -- authorize() below checks it explicitly instead.
  @Get("/authorize")
  @AuthorizeExpression("#principal != null")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse authorize(
      HttpRequest request, @Param("privilege") Optional<String> privilege) {
    Identity identity =
        serverProperties.isAuthorizationEnabled() ? verifiedIdentity() : leniantIdentity(request);
    // OWNER is OR'd in explicitly, matching every other @AuthorizeExpression in this server:
    // Casbin's policy tuples are literal, and grant one specific (principal, resource, privilege)
    // at a time -- OWNER on the metastore does not imply CREATE_SHARE unless something says so.
    boolean authorized =
        privilege.isEmpty()
            ? true
            : authorizer.authorizeAny(
                identity.id(),
                metastoreRepository.getMetastoreId(),
                Privileges.OWNER,
                privilegeOf(privilege.get()));
    return HttpResponse.ofJson(
        Map.of(
            "user_id", identity.id().toString(),
            "user_name", identity.name(),
            "authorized", authorized));
  }

  /**
   * The identity {@code AuthDecorator} already verified for this request — a valid, UC-signed token
   * naming an {@code ENABLED} user, or this method is never reached at all: a request that failed
   * either check was already answered with 401 before now.
   */
  private Identity verifiedIdentity() {
    UUID principalId = userRepository.findPrincipalId();
    if (principalId == null) {
      // AuthDecorator runs ahead of this handler and rejects an unauthenticated request itself,
      // so reaching here with no principal would be this server's own bug, not the caller's.
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "no principal on this request");
    }
    return new Identity(principalId, userRepository.getUser(principalId.toString()).getEmail());
  }

  /**
   * No decorator ran, so nothing about this token has been checked. Read the same {@code sub} claim
   * a verified identity comes from, without verifying the signature — there is nothing to verify
   * against in this mode, only something to read — falling back to the token text itself when it
   * does not even decode as a JWT (a plain configured string, say). The user id is derived
   * deterministically from whichever of those two a given token yields, so the same token always
   * resolves to the same id across calls without needing a database row for it.
   */
  private Identity leniantIdentity(HttpRequest request) {
    String token = bearerToken(request);
    if (token == null) {
      throw new AuthorizationException(ErrorCode.UNAUTHENTICATED, "no bearer token presented");
    }
    String subject = token;
    try {
      DecodedJWT decoded = JWT.decode(token);
      String claimed = decoded.getClaim(JwtClaim.SUBJECT.key()).asString();
      if (claimed != null && !claimed.isBlank()) {
        subject = claimed;
      }
    } catch (JWTDecodeException e) {
      LOGGER.debug("OpenSharing authorize: token is not JWT-shaped, using it as the subject", e);
    }
    return new Identity(UUID.nameUUIDFromBytes(subject.getBytes(StandardCharsets.UTF_8)), subject);
  }

  private static String bearerToken(HttpRequest request) {
    String header = request.headers().get(HttpHeaderNames.AUTHORIZATION);
    return header != null && header.startsWith(BEARER_PREFIX)
        ? header.substring(BEARER_PREFIX.length()).trim()
        : null;
  }

  private static Privileges privilegeOf(String privilege) {
    try {
      return Privileges.valueOf(privilege);
    } catch (IllegalArgumentException e) {
      throw new AuthorizationException(
          ErrorCode.INVALID_ARGUMENT, "unknown privilege: " + privilege);
    }
  }

  private record Identity(UUID id, String name) {}
}
