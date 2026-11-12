package io.unitycatalog.server.service.sharing;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SHARE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.server.annotation.StatusCode;
import io.opensharing.auth.UserContext;
import io.opensharing.http.ApiException;
import io.opensharing.http.ListResponse;
import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.CreateShareRequest;
import io.opensharing.share.ShareResponse;
import io.opensharing.share.ShareService;
import io.opensharing.share.UpdateShareRequest;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.RegisteredService;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * OpenSharing's provider API for shares, served by Unity Catalog. UC authenticates callers and
 * authorizes each method by its expression, for both sharing privileges and ownership, per item for
 * listings. So the connector grants every privilege OpenSharing asks it for, and OpenSharing's own
 * ownership checks agree with UC's. Share owners are recorded in UC's authorizer, so expressions
 * can name them. Bodies and errors keep OpenSharing's wire format: the server registers this
 * service with OpenSharing's JSON mapper and {@link OpenSharingExceptionHandler}.
 */
@ProducesJson
public final class OpenSharingProviderService extends AuthorizedService
    implements RegisteredService {

  private static final String OWN_SHARE = "#authorize(#principal, #share, OWNER)";

  /** Transferring a share's ownership: the ownerId the body sets, checked by catalog admins. */
  private static final String TRANSFER_SHARE =
      """
      #ownerId != null && #authorize(#principal, #metastore, OWNER)
      """;

  private static final String READ_SHARE =
      """
      #authorizeAny(#principal, #metastore, OWNER, USE_SHARE) ||
      #authorize(#principal, #share, OWNER)
      """;

  private final ShareService shares;
  private final MetastoreRepository metastore;
  private final HibernateTransactions transactions;
  private final OpenSharingExceptionHandler exceptionHandler = new OpenSharingExceptionHandler();

  OpenSharingProviderService(
      OpenSharing openSharing,
      HibernateTransactions transactions,
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.shares = openSharing.shares();
    this.metastore = repositories.getMetastoreRepository();
    this.transactions = transactions;
  }

  /**
   * {@code POST /shares}: creates a share owned by the caller. The share and its owner record in
   * UC's authorizer are one unit of work: when recording the owner fails, the share is not created.
   */
  @Post("/shares")
  @StatusCode(201)
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_SHARE)")
  @AuthorizeResourceKey(METASTORE)
  public ShareResponse createShare(CreateShareRequest request) {
    return transactions.inTransaction(
        false,
        session -> {
          ShareResponse created = shares.create(user(), request);
          initializeBasicAuthorization(created.id());
          return created;
        });
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
  public ListResponse<ShareResponse> listShares(
      @Param("maxResults") Optional<Integer> maxResults,
      @Param("pageToken") Optional<String> pageToken) {
    return visible(SHARE, shares.listAll(maxResults.orElse(null), pageToken.orElse(null)));
  }

  /**
   * {@code GET /shares/{share}}: gets a share by name in any case, with the tables in it when
   * {@code include_shared_data} is true. Owner or USE SHARE.
   */
  @Get("/shares/{share}")
  @AuthorizeExpression(READ_SHARE)
  @AuthorizeResourceKey(METASTORE)
  public ShareResponse getShare(
      @Param("share") @AuthorizeResourceKey(SHARE) String share,
      @Param("include_shared_data") @Default("false") boolean includeSharedData) {
    return shares.get(user(), share, includeSharedData);
  }

  /**
   * {@code PATCH /shares/{share}}: applies the table removes, then the adds, then the other fields
   * set in the body, all or nothing. Owner only; a catalog admin may instead transfer the ownership
   * to the body's {@code ownerId}, in a request that carries no other change, and UC's owner record
   * moves with it.
   */
  @Patch("/shares/{share}")
  @AuthorizeExpression(OWN_SHARE + " || " + TRANSFER_SHARE)
  @AuthorizeResourceKey(METASTORE)
  public ShareResponse updateShare(
      @Param("share") @AuthorizeResourceKey(SHARE) String share,
      @AuthorizeKey(key = "ownerId") UpdateShareRequest request) {
    return transactions.inTransaction(
        false,
        session -> {
          String ownerBefore = shares.get(user(), share, false).ownerId();
          ShareResponse updated = shares.update(user(), share, request);
          if (request.ownerId() != null) {
            transferOwnership(updated.id(), ownerBefore, request.ownerId());
          }
          return updated;
        });
  }

  /**
   * {@code DELETE /shares/{share}}: deletes the share and its owner record in UC's authorizer as
   * one unit of work. Owner only.
   */
  @Delete("/shares/{share}")
  @AuthorizeExpression(OWN_SHARE)
  public HttpResponse deleteShare(@Param("share") @AuthorizeResourceKey(SHARE) String share) {
    UUID id = id(SHARE, share);
    transactions.inTransaction(
        false,
        session -> {
          shares.delete(user(), share);
          removeAuthorizations(id.toString());
          return null;
        });
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
  private <T> ListResponse<T> visible(SecurableType type, ListResponse<T> page) {
    List<T> items = new ArrayList<>(page.items());
    applyResponseFilter(type, items);
    return new ListResponse<>(items, page.nextPageToken());
  }

  /** The caller UC's auth decorator authenticated, as OpenSharing's durable owner identity. */
  private UserContext user() {
    UUID id = userRepository.findPrincipalId();
    if (id == null) {
      throw ApiException.unauthenticated("Unity Catalog did not authenticate the caller");
    }
    // OpenSharing trusts the admin flag as given, like the id: a metastore OWNER may transfer the
    // ownership of any share or recipient.
    boolean admin =
        authorizer.authorize(
            id, SecurableType.METASTORE, metastore.getMetastoreId(), Privileges.OWNER);
    return new UserContext(id.toString(), null, IdentityUtils.findPrincipalEmailAddress(), admin);
  }

  /** Moves UC's OWNER record to the new owner, after OpenSharing moved the ownership itself. */
  private void transferOwnership(String resourceId, String oldOwnerId, String newOwnerId) {
    authorizer.revokeAuthorization(
        UUID.fromString(oldOwnerId), UUID.fromString(resourceId), Privileges.OWNER);
    authorizer.grantAuthorization(
        UUID.fromString(newOwnerId), UUID.fromString(resourceId), Privileges.OWNER);
  }
}
