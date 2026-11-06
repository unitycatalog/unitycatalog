package io.unitycatalog.server.sharing;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SHARE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.opensharing.auth.UserContext;
import io.opensharing.http.ApiException;
import io.opensharing.http.ApiJson;
import io.opensharing.http.ListResponse;
import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.CreateShareRequest;
import io.opensharing.share.ShareResponse;
import io.opensharing.share.ShareService;
import io.opensharing.share.UpdateShareRequest;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * OpenSharing's provider API for shares, served as a UC REST service. UC authenticates callers and
 * authorizes each method by its expression, for both sharing privileges and ownership, per item for
 * listings. So the connector grants every privilege OpenSharing asks it for, and OpenSharing's own
 * ownership checks agree with UC's. Share owners are recorded in UC's authorizer, so expressions
 * can name them. Bodies and errors keep OpenSharing's wire format, so they are read and written
 * with OpenSharing's JSON mapper.
 */
public final class OpenSharingProviderService extends AuthorizedService
    implements UnityCatalogRestService {

  private static final String OWN_SHARE = "#authorize(#principal, #share, OWNER)";

  private static final String READ_SHARE =
      """
      #authorizeAny(#principal, #metastore, OWNER, USE_SHARE) ||
      #authorize(#principal, #share, OWNER)
      """;

  private final ShareService shares;
  private final OpenSharingExceptionHandler exceptionHandler = new OpenSharingExceptionHandler();

  OpenSharingProviderService(
      OpenSharing openSharing,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.shares = openSharing.shares();
  }

  /** {@code POST /shares}: creates a share owned by the caller. */
  @Post("/shares")
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_SHARE)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createShare(JsonNode body) throws JsonProcessingException {
    ShareResponse created = shares.create(user(), read(body, CreateShareRequest.class));
    initializeBasicAuthorization(created.id());
    return json(HttpStatus.CREATED, created);
  }

  /**
   * {@code GET /shares}: lists every share with USE SHARE, otherwise the caller's own. Each page is
   * filtered per share after it is read, as UC's own listings are, so a page can come back short or
   * empty with more to follow.
   */
  @Get("/shares")
  @AuthorizeExpression(READ_SHARE)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listShares(
      @Param("maxResults") Optional<Integer> maxResults,
      @Param("pageToken") Optional<String> pageToken) {
    ListResponse<ShareResponse> page =
        shares.listAll(maxResults.orElse(null), pageToken.orElse(null));
    return json(HttpStatus.OK, filter(SHARE, page, ShareResponse::id));
  }

  /**
   * {@code GET /shares/{share}}: gets a share by name in any case, with the tables in it when
   * {@code include_shared_data} is true. Owner or USE SHARE.
   */
  @Get("/shares/{share}")
  @AuthorizeExpression(READ_SHARE)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getShare(
      @Param("share") @AuthorizeResourceKey(SHARE) String share,
      @Param("include_shared_data") @Default("false") boolean includeSharedData) {
    return json(HttpStatus.OK, shares.get(user(), share, includeSharedData));
  }

  /**
   * {@code PATCH /shares/{share}}: applies the table removes, then the adds, then the other fields
   * set in the body, all or nothing. Owner only.
   */
  @Patch("/shares/{share}")
  @AuthorizeExpression(OWN_SHARE)
  public HttpResponse updateShare(
      @Param("share") @AuthorizeResourceKey(SHARE) String share, JsonNode body)
      throws JsonProcessingException {
    return json(HttpStatus.OK, shares.update(user(), share, read(body, UpdateShareRequest.class)));
  }

  /** {@code DELETE /shares/{share}}: deletes the share. Owner only. */
  @Delete("/shares/{share}")
  @AuthorizeExpression(OWN_SHARE)
  public HttpResponse deleteShare(@Param("share") @AuthorizeResourceKey(SHARE) String share) {
    UUID id = id(SHARE, share);
    shares.delete(user(), share);
    removeAuthorizations(id.toString());
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Override
  public ExceptionHandlerFunction exceptionHandler() {
    return exceptionHandler;
  }

  private UUID id(SecurableType type, String name) {
    return keyMapper.mapResourceKeys(Map.<SecurableType, Object>of(type, name)).get(type);
  }

  /** The items of {@code page} the caller may see, by each method's expression. */
  private <T> ListResponse<T> filter(
      SecurableType type, ListResponse<T> page, Function<T, String> id) {
    List<String> visible = new ArrayList<>(page.items().stream().map(id).toList());
    applyResponseFilter(type, visible);
    Set<String> kept = Set.copyOf(visible);
    return new ListResponse<>(
        page.items().stream().filter(item -> kept.contains(id.apply(item))).toList(),
        page.nextPageToken());
  }

  /** The caller UC's auth decorator authenticated, as OpenSharing's durable owner identity. */
  private UserContext user() {
    UUID id = userRepository.findPrincipalId();
    if (id == null) {
      throw ApiException.unauthenticated("Unity Catalog did not authenticate the caller");
    }
    return UserContext.fromUserIdAndName(id.toString(), IdentityUtils.findPrincipalEmailAddress());
  }

  private static <T> T read(JsonNode body, Class<T> type) throws JsonProcessingException {
    if (body == null || !body.isObject()) {
      throw ApiException.invalidParameter("the request body must be a JSON object");
    }
    return ApiJson.MAPPER.treeToValue(body, type);
  }

  private static HttpResponse json(HttpStatus status, Object body) {
    return ApiJson.response(ResponseHeaders.of(status), body);
  }
}
