package io.unitycatalog.server.service;

import static io.unitycatalog.server.auth.AuthorizeExpressions.SCIM_PROVISIONING;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.utils.Scim2Utils.asUserResource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.Produces;
import com.linecorp.armeria.server.annotation.Put;
import com.linecorp.armeria.server.annotation.StatusCode;
import com.unboundid.scim2.common.exceptions.BadRequestException;
import com.unboundid.scim2.common.exceptions.PreconditionFailedException;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.exceptions.ScimException;
import com.unboundid.scim2.common.exceptions.ServerErrorException;
import com.unboundid.scim2.common.filters.Filter;
import com.unboundid.scim2.common.messages.ListResponse;
import com.unboundid.scim2.common.messages.PatchOpType;
import com.unboundid.scim2.common.messages.PatchOperation;
import com.unboundid.scim2.common.messages.PatchRequest;
import com.unboundid.scim2.common.types.Email;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.types.UserResource;
import com.unboundid.scim2.common.utils.FilterEvaluator;
import com.unboundid.scim2.common.utils.Parser;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.Scim2Utils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * SCIM2-compliant user management.
 *
 * <p>This will be a SCIM 2.0 compliant user management service. The UC User data model will be a
 * minimal set of required fields that are necessary to support user management/exchange.
 *
 * <ul>
 *   <li>id - internal unique identifier for user
 *   <li>name - maps to SCIM displayName
 *   <li>email - maps to SCIM primary email
 *   <li>externalId - maps to SCIM external id
 *   <li>userName - maps to SCIM primary email
 * </ul>
 *
 * <p>A user is owned by the admin, or by the IdP whose SCIM token created it, and only its owner
 * changes or deletes it. An IdP sees only its own users; any other is not found for it.
 */
public class Scim2UserService implements ScimService {
  private final UserRepository userRepository;
  private final UnityCatalogAuthorizer authorizer;
  private final MetastoreRepository metastoreRepository;
  private final ServerProperties serverProperties;

  public Scim2UserService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    this.authorizer = authorizer;
    this.serverProperties = serverProperties;
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Get("")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<UserResource> getScimUsers(
      @Param("filter") Optional<String> filter,
      @Param("startIndex") Optional<Integer> startIndex,
      @Param("count") Optional<Integer> count) {
    final Filter userFilter =
        filter.filter(f -> !f.isEmpty()).<Filter>map(this::parseFilter).orElse(null);
    FilterEvaluator filterEvaluator = new FilterEvaluator();
    String caller = IdentityUtils.callerOwner();

    List<UserResource> userResourcesList =
        userRepository
            .listUsers(
                startIndex.orElse(1) - 1,
                count.orElse(50),
                m -> isVisible(m, caller) && match(filterEvaluator, userFilter, asUserResource(m)))
            .stream()
            .map(Scim2Utils::asUserResource)
            .toList();

    Meta meta = new Meta();
    meta.setCreated(Calendar.getInstance());
    meta.setLastModified(Calendar.getInstance());
    meta.setResourceType("User");

    ListResponse<UserResource> userResources =
        new ListResponse<>(
            userResourcesList.size(),
            userResourcesList,
            startIndex.orElse(1),
            userResourcesList.size());
    userResources.setMeta(meta);

    return userResources;
  }

  @Post("")
  @Produces("application/scim+json")
  @StatusCode(201)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public UserResource createScimUser(UserResource userResource) {
    // Get primary email address
    Email primaryEmail =
        userResource.getEmails().stream()
            .filter(Email::getPrimary)
            .findFirst()
            .orElseThrow(
                () ->
                    new Scim2RuntimeException(
                        new PreconditionFailedException("User does not have a primary email.")));

    String pictureUrl = "";
    if (userResource.getPhotos() != null && !userResource.getPhotos().isEmpty()) {
      pictureUrl = userResource.getPhotos().get(0).getValue().toString();
    }
    // Outside the try: a refused SCIM token is a 403, not a bad request.
    String owner = IdentityUtils.callerOwner();
    try {
      User user =
          userRepository.createUser(
              CreateUser.builder()
                  .name(userResource.getDisplayName())
                  .email(primaryEmail.getValue())
                  .active(userResource.getActive())
                  .externalId(userResource.getExternalId())
                  .pictureUrl(pictureUrl)
                  .owner(owner)
                  .build());
      return asUserResource(user);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.ALREADY_EXISTS) {
        throw new Scim2RuntimeException(new ResourceConflictException(e.getMessage()));
      } else if (e.getErrorCode().getHttpStatus().isServerError()) {
        // A server failure, such as a database error, is not the caller's fault.
        throw new Scim2RuntimeException(new ServerErrorException(e.getMessage(), null, e));
      } else {
        throw new Scim2RuntimeException(new BadRequestException(e.getMessage()));
      }
    }
  }

  @Get("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public UserResource getUser(@Param("id") String id) {
    return asUserResource(visibleUser(id, IdentityUtils.callerOwner()));
  }

  @Put("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public UserResource updateUser(@Param("id") String id, UserResource userResource) {
    User existing = changeableUser(id);
    if (!id.equals(userResource.getId())) {
      throw new Scim2RuntimeException(new ResourceConflictException("User id mismatch."));
    }
    if (Boolean.FALSE.equals(userResource.getActive())) {
      checkNotMetastoreOwner(existing);
    }
    checkExternalIdUnchangedForScimToken(existing, userResource.getExternalId());
    checkEmailUnchangedForScimToken(existing, userResource);

    UpdateUser updateUser =
        UpdateUser.builder()
            .name(userResource.getDisplayName())
            .active(userResource.getActive())
            .externalId(userResource.getExternalId())
            .build();

    return asUserResource(userRepository.updateUser(id, updateUser));
  }

  @Delete("/{id}")
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteUser(@Param("id") String id) {
    User user = changeableUser(id);
    checkNotMetastoreOwner(user);
    authorizer.clearAuthorizationsForPrincipal(
        UUID.fromString(Objects.requireNonNull(user.getId())));
    userRepository.deleteUser(user.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  @Patch("/{id}")
  @AuthorizeExpression(SCIM_PROVISIONING)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse patchUser(@Param("id") String id, PatchRequest patchRequest) {
    return patchRequest.getOperations().stream()
        .filter(
            op ->
                op.getOpType() == PatchOpType.REPLACE
                    && op.getPath() == null) // Only support patch for okta
        .findFirst()
        .map(op -> handleUserUpdate(id, op))
        .orElse(HttpResponse.of(HttpStatus.NOT_IMPLEMENTED));
  }

  private HttpResponse handleUserUpdate(String id, PatchOperation operation) {
    try {
      User user = changeableUser(id);
      Boolean value = operation.getValues(Boolean.class).get(0);
      if (Boolean.FALSE.equals(value)) {
        checkNotMetastoreOwner(user);
      }
      UpdateUser updateUser = UpdateUser.builder().active(value).build();
      userRepository.updateUser(id, updateUser);
      return HttpResponse.of(HttpStatus.OK);
    } catch (ScimException | JsonProcessingException e) {
      return handleExceptionDuringPatch(e);
    }
  }

  /** The admin sees every user; an IdP sees only its own. */
  private static boolean isVisible(User user, String caller) {
    return IdentityUtils.ADMIN_OWNER.equals(caller) || caller.equals(user.getOwner());
  }

  /** The user, if {@code caller} may see it; another owner's user is not found for an IdP. */
  private User visibleUser(String id, String caller) {
    User user = userRepository.getUser(id);
    if (!isVisible(user, caller)) {
      throw new BaseException(ErrorCode.NOT_FOUND, "User not found: " + id);
    }
    return user;
  }

  /** The user, if the caller owns it and may change it. */
  private User changeableUser(String id) {
    String caller = IdentityUtils.callerOwner();
    User user = visibleUser(id, caller);
    checkNotIdpPrincipalForScimToken(user);
    if (!caller.equals(user.getOwner())) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "User " + user.getEmail() + " is managed by identity provider " + user.getOwner());
    }
    return user;
  }

  // With authorization disabled, every authorize() call returns true, so the owner check only
  // means something when authorization is enabled.
  private void checkNotMetastoreOwner(User user) {
    if (!serverProperties.isAuthorizationEnabled()) {
      return;
    }
    UUID metastoreId = metastoreRepository.getMetastoreId();
    UUID userId = UUID.fromString(user.getId());
    if (authorizer.authorize(userId, METASTORE, metastoreId, Privileges.OWNER)) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "The metastore owner can't be deleted or deactivated: " + user.getEmail());
    }
  }

  // Only the admin changes an IdP principal: with a SCIM token, one IdP could lock out or revive
  // another IdP's principal, or its own. A principal is a user the admin owns (bin/uc-admin-token
  // refuses others), so a SCIM token already gets 404 for it; this keeps the rule for a principal
  // row an IdP owns. SCIM tokens are honored only with authorization enabled.
  private void checkNotIdpPrincipalForScimToken(User user) {
    if (!serverProperties.isAuthorizationEnabled()) {
      return;
    }
    if (IdentityUtils.isScimToken() && user.getIdpName() != null) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "An IdP principal can't be changed with a SCIM token: " + user.getEmail());
    }
  }

  // Token exchange resolves an OAuth client to the user with that external id, so a SCIM token
  // that relinks an existing user could take it over. A null external id leaves it unchanged.
  // Okta's PUT resends the external id it set.
  // TODO(#2202): replace with the owner check (an IdP edits only rows it owns).
  private void checkExternalIdUnchangedForScimToken(User user, String externalId) {
    if (!serverProperties.isAuthorizationEnabled()) {
      return;
    }
    if (IdentityUtils.isScimToken()
        && externalId != null
        && !externalId.equals(user.getExternalId())) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "A SCIM token can't change the external id of a user: " + user.getEmail());
    }
  }

  // Token exchange signs in a user by email, so a SCIM token that changes a user's email could
  // take it over. The match is exact, so a case variant is refused, and so are emails with no
  // primary value. A PUT that sends the stored email again as the primary email, or sends no
  // emails, is accepted: Okta's PUT resends the email it set. PATCH changes only active.
  // TODO(#2202): replace with the owner check (an IdP edits only rows it owns).
  private void checkEmailUnchangedForScimToken(User user, UserResource userResource) {
    if (!serverProperties.isAuthorizationEnabled()) {
      return;
    }
    List<Email> emails = userResource.getEmails();
    if (!IdentityUtils.isScimToken() || emails == null || emails.isEmpty()) {
      return;
    }
    String primaryEmail =
        emails.stream()
            .filter(email -> email != null && Boolean.TRUE.equals(email.getPrimary()))
            .findFirst()
            .map(Email::getValue)
            .orElse(null);
    if (!Objects.equals(primaryEmail, user.getEmail())) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "A SCIM token can't change the email of a user: " + user.getEmail());
    }
  }

  private HttpResponse handleExceptionDuringPatch(Exception ex) {
    if (ex instanceof ScimException) {
      throw new Scim2RuntimeException((ScimException) ex);
    } else {
      throw new Scim2RuntimeException(
          new ServerErrorException("Problem with patch operation", ex.getMessage(), ex));
    }
  }

  private Filter parseFilter(String filter) {
    try {
      return Parser.parseFilter(filter);
    } catch (BadRequestException e) {
      throw new Scim2RuntimeException(e);
    }
  }

  private boolean match(FilterEvaluator filterEvaluator, Filter userFilter, UserResource user) {
    try {
      return (userFilter == null
          || userFilter.visit(filterEvaluator, user.asGenericScimResource().getObjectNode()));
    } catch (ScimException e) {
      throw new Scim2RuntimeException(e);
    }
  }
}
