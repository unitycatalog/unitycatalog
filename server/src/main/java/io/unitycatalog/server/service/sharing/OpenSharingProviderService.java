package io.unitycatalog.server.service.sharing;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.RECIPIENT;
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
import io.opensharing.recipient.CreateRecipientRequest;
import io.opensharing.recipient.IssuedTokenResponse;
import io.opensharing.recipient.RecipientResponse;
import io.opensharing.recipient.RecipientService;
import io.opensharing.recipient.RotateTokenRequest;
import io.opensharing.recipient.UpdateRecipientRequest;
import io.opensharing.runtime.OpenSharing;
import io.opensharing.share.CreateShareRequest;
import io.opensharing.share.SharePermissionResponse;
import io.opensharing.share.ShareResponse;
import io.opensharing.share.ShareService;
import io.opensharing.share.UpdateSharePermissionsRequest;
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
 * OpenSharing's provider API for shares, their recipient grants, and recipients, served by Unity
 * Catalog. UC authenticates callers and authorizes each method by its expression, for both sharing
 * privileges and ownership: per item for listings, and of the recipient for a grant. So the
 * connector grants every privilege OpenSharing asks it for, and OpenSharing's own ownership checks
 * agree with UC's. Share and recipient owners are recorded in UC's authorizer, so expressions can
 * name them. Bodies and errors keep OpenSharing's wire format: the server registers this service
 * with OpenSharing's JSON mapper and {@link OpenSharingExceptionHandler}.
 */
@ProducesJson
public final class OpenSharingProviderService extends AuthorizedService
    implements RegisteredService {

  private static final String OWN_SHARE = "#authorize(#principal, #share, OWNER)";

  /** Transferring a share's or recipient's ownership: the ownerId the body sets, admin only. */
  private static final String TRANSFER =
      """
      #ownerId != null && #authorize(#principal, #metastore, OWNER)
      """;

  private static final String OWN_RECIPIENT = "#authorize(#principal, #recipient, OWNER)";

  private static final String READ_SHARE =
      """
      #authorizeAny(#principal, #metastore, OWNER, USE_SHARE) ||
      #authorize(#principal, #share, OWNER)
      """;

  private static final String READ_RECIPIENT =
      """
      #authorizeAny(#principal, #metastore, OWNER, USE_RECIPIENT) ||
      #authorize(#principal, #recipient, OWNER)
      """;

  /** {@code #recipient} is the one recipient a request grants to, or null for none. */
  private static final String GRANT_SHARE =
      """
      #authorize(#principal, #share, OWNER) && (#recipient == null ||
      #authorizeAny(#principal, #metastore, OWNER, USE_RECIPIENT) ||
      #authorize(#principal, #recipient, OWNER))
      """;

  private final ShareService shares;
  private final RecipientService recipients;
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
    this.recipients = openSharing.recipients();
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
  @AuthorizeExpression(OWN_SHARE + " || " + TRANSFER)
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

  /**
   * {@code GET /shares/{share}/permissions}: lists which recipients the share is granted to. Owner
   * or USE SHARE.
   */
  @Get("/shares/{share}/permissions")
  @AuthorizeExpression(READ_SHARE)
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<SharePermissionResponse> listSharePermissions(
      @Param("share") @AuthorizeResourceKey(SHARE) String share,
      @Param("maxResults") Optional<Integer> maxResults,
      @Param("pageToken") Optional<String> pageToken) {
    return shares.listPermissions(user(), share, maxResults.orElse(null), pageToken.orElse(null));
  }

  /**
   * {@code PATCH /shares/{share}/permissions}: grants and revokes recipients. Owner only, and
   * granting to a recipient takes what getting it does: owning it, or USE RECIPIENT. A request
   * grants to one recipient at most. Revoking takes nothing of the recipient, so an owner can
   * always cut one off.
   */
  @Patch("/shares/{share}/permissions")
  @AuthorizeExpression(GRANT_SHARE)
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<SharePermissionResponse> updateSharePermissions(
      @Param("share") @AuthorizeResourceKey(SHARE) String share,
      @AuthorizeResourceKey(value = RECIPIENT, extractor = GrantedRecipientExtractor.class)
          UpdateSharePermissionsRequest request) {
    return shares.updatePermissions(user(), share, request);
  }

  /**
   * {@code POST /recipients}: creates a recipient owned by the caller. The recipient and its owner
   * record in UC's authorizer are one unit of work, like a share's.
   */
  @Post("/recipients")
  @StatusCode(201)
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER, CREATE_RECIPIENT)")
  @AuthorizeResourceKey(METASTORE)
  public RecipientResponse createRecipient(CreateRecipientRequest request) {
    return transactions.inTransaction(
        false,
        session -> {
          RecipientResponse created = recipients.create(user(), request);
          initializeBasicAuthorization(created.id());
          return created;
        });
  }

  /**
   * {@code GET /recipients}: lists every recipient with USE RECIPIENT, otherwise the caller's.
   * Filtered per recipient after paging, like {@link #listShares}.
   */
  @Get("/recipients")
  @AuthorizeExpression(READ_RECIPIENT)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<RecipientResponse> listRecipients(
      @Param("maxResults") Optional<Integer> maxResults,
      @Param("pageToken") Optional<String> pageToken) {
    return visible(RECIPIENT, recipients.listAll(maxResults.orElse(null), pageToken.orElse(null)));
  }

  /** {@code GET /recipients/{recipient}}: gets a recipient by name. Owner or USE RECIPIENT. */
  @Get("/recipients/{recipient}")
  @AuthorizeExpression(READ_RECIPIENT)
  @AuthorizeResourceKey(METASTORE)
  public RecipientResponse getRecipient(
      @Param("recipient") @AuthorizeResourceKey(RECIPIENT) String recipient) {
    return recipients.get(user(), recipient);
  }

  /**
   * {@code PATCH /recipients/{recipient}}: updates the fields set in the body. Owner only; a
   * catalog admin may instead transfer the ownership to the body's {@code ownerId}, in a request
   * that carries no other change, and UC's owner record moves with it.
   */
  @Patch("/recipients/{recipient}")
  @AuthorizeExpression(OWN_RECIPIENT + " || " + TRANSFER)
  @AuthorizeResourceKey(METASTORE)
  public RecipientResponse updateRecipient(
      @Param("recipient") @AuthorizeResourceKey(RECIPIENT) String recipient,
      @AuthorizeKey(key = "ownerId") UpdateRecipientRequest request) {
    return transactions.inTransaction(
        false,
        session -> {
          String ownerBefore = recipients.get(user(), recipient).ownerId();
          RecipientResponse updated = recipients.update(user(), recipient, request);
          if (request.ownerId() != null) {
            transferOwnership(updated.id(), ownerBefore, request.ownerId());
          }
          return updated;
        });
  }

  /**
   * {@code DELETE /recipients/{recipient}}: deletes the recipient, its tokens and its owner record
   * in UC's authorizer as one unit of work. Owner only.
   */
  @Delete("/recipients/{recipient}")
  @AuthorizeExpression(OWN_RECIPIENT)
  public HttpResponse deleteRecipient(
      @Param("recipient") @AuthorizeResourceKey(RECIPIENT) String recipient) {
    UUID id = id(RECIPIENT, recipient);
    transactions.inTransaction(
        false,
        session -> {
          recipients.delete(user(), recipient);
          removeAuthorizations(id.toString());
          return null;
        });
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  /**
   * {@code POST /recipients/{recipient}/rotate-token}: issues a replacement token with a new
   * activation link. Owner only.
   */
  @Post("/recipients/{recipient}/rotate-token")
  @StatusCode(201)
  @AuthorizeExpression(OWN_RECIPIENT)
  public IssuedTokenResponse rotateRecipientToken(
      @Param("recipient") @AuthorizeResourceKey(RECIPIENT) String recipient,
      RotateTokenRequest request) {
    return recipients.rotateToken(user(), recipient, request);
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
