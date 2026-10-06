package io.unitycatalog.server.sharing;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.opensharing.auth.UserContext;
import io.opensharing.http.ApiException;
import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.CreateShareRequest;
import io.opensharing.share.ShareService;
import io.opensharing.share.UpdateShareRequest;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.IdentityUtils;
import java.util.UUID;

/**
 * OpenSharing's provider share API, served as a UC REST service. UC authenticates callers and
 * checks each method's expression; OpenSharing checks share ownership. Bodies and errors keep
 * OpenSharing's wire format.
 */
public final class OpenSharingShareService implements UnityCatalogRestService {

  private static final String AUTHENTICATED = "#principal != null";

  private static final ObjectMapper MAPPER =
      JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build();

  private final ShareService shares;
  private final UserRepository users;
  private final OpenSharingExceptionHandler exceptionHandler;

  public OpenSharingShareService(OpenSharing openSharing, UserRepository users) {
    this.shares = openSharing.shares();
    this.users = users;
    this.exceptionHandler = new OpenSharingExceptionHandler(MAPPER);
  }

  /** {@code POST /shares}: creates a share owned by the caller. */
  @Post("/shares")
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_SHARE)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse create(CreateShareRequest request) {
    return json(HttpStatus.CREATED, shares.create(user(), request));
  }

  /** {@code GET /shares}: lists every share by name, unpaged. */
  @Get("/shares")
  @AuthorizeExpression(AUTHENTICATED)
  public HttpResponse list() {
    return json(HttpStatus.OK, shares.list());
  }

  /** {@code GET /shares/{share}}: gets a share by name in any case. */
  @Get("/shares/{share}")
  @AuthorizeExpression(AUTHENTICATED)
  public HttpResponse get(@Param("share") String share) {
    return json(HttpStatus.OK, shares.get(share));
  }

  /** {@code PATCH /shares/{share}}: updates the fields set in the body. Owner only. */
  @Patch("/shares/{share}")
  @AuthorizeExpression(AUTHENTICATED)
  public HttpResponse update(@Param("share") String share, UpdateShareRequest request) {
    return json(HttpStatus.OK, shares.update(user(), share, request));
  }

  /** {@code DELETE /shares/{share}}: deletes the share. Owner only. */
  @Delete("/shares/{share}")
  @AuthorizeExpression(AUTHENTICATED)
  public HttpResponse delete(@Param("share") String share) {
    shares.delete(user(), share);
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Override
  public ExceptionHandlerFunction exceptionHandler() {
    return exceptionHandler;
  }

  /** The caller UC's auth decorator authenticated, as OpenSharing's durable owner identity. */
  private UserContext user() {
    UUID id = users.findPrincipalId();
    if (id == null) {
      throw ApiException.unauthenticated("Unity Catalog did not authenticate the caller");
    }
    return UserContext.fromUserIdAndName(
        id.toString(), IdentityUtils.findPrincipalEmailAddress());
  }

  private static HttpResponse json(HttpStatus status, Object body) {
    try {
      return HttpResponse.of(status, MediaType.JSON_UTF_8, MAPPER.writeValueAsBytes(body));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to write an OpenSharing response", e);
    }
  }
}
