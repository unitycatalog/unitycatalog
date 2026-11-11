package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.utils.Scim2Utils.asUserResource;

import com.fasterxml.jackson.databind.JsonNode;
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
import com.unboundid.scim2.common.types.Name;
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
import io.unitycatalog.server.utils.Scim2Utils;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 */
public class Scim2UserService implements ScimService {
  private static final String ACTIVE = "active";
  private static final String CORE_USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User:";
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
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public ListResponse<UserResource> getScimUsers(
      @Param("filter") Optional<String> filter,
      @Param("startIndex") Optional<Integer> startIndex,
      @Param("count") Optional<Integer> count) {
    final Filter userFilter =
        filter.filter(f -> !f.isEmpty()).<Filter>map(this::parseFilter).orElse(null);
    FilterEvaluator filterEvaluator = new FilterEvaluator();

    // totalResults is the number of matches, so the page is cut after matching (RFC 7644 3.4.2).
    List<User> matches =
        userRepository.listUsers(m -> match(filterEvaluator, userFilter, asUserResource(m)));
    int firstIndex = Math.max(startIndex.orElse(1), 1);
    List<UserResource> userResourcesList =
        matches.stream()
            .skip(firstIndex - 1)
            .limit(Math.max(count.orElse(50), 0))
            .map(Scim2Utils::asUserResource)
            .toList();

    Meta meta = new Meta();
    meta.setCreated(Calendar.getInstance());
    meta.setLastModified(Calendar.getInstance());
    meta.setResourceType("User");

    ListResponse<UserResource> userResources =
        new ListResponse<>(matches.size(), userResourcesList, firstIndex, userResourcesList.size());
    userResources.setMeta(meta);

    return userResources;
  }

  @Post("")
  @Produces("application/scim+json")
  @StatusCode(201)
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
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

    String name =
        userName(userResource)
            .orElseThrow(
                () ->
                    new Scim2RuntimeException(
                        new BadRequestException("A user needs displayName or name.")));

    String pictureUrl = "";
    if (userResource.getPhotos() != null && !userResource.getPhotos().isEmpty()) {
      pictureUrl = userResource.getPhotos().get(0).getValue().toString();
    }
    try {
      User user =
          userRepository.createUser(
              CreateUser.builder()
                  .name(name)
                  .email(primaryEmail.getValue())
                  .active(userResource.getActive())
                  .externalId(userResource.getExternalId())
                  .pictureUrl(pictureUrl)
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
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public UserResource getUser(@Param("id") String id) {
    return asUserResource(userRepository.getUser(id));
  }

  @Put("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public UserResource updateUser(@Param("id") String id, UserResource userResource) {
    User existing = userRepository.getUser(id);
    if (!id.equals(userResource.getId())) {
      throw new Scim2RuntimeException(new ResourceConflictException("User id mismatch."));
    }
    if (Boolean.FALSE.equals(userResource.getActive())) {
      checkNotMetastoreOwner(existing);
    }

    UpdateUser updateUser =
        UpdateUser.builder()
            .name(userName(userResource).orElse(null))
            .active(userResource.getActive())
            .externalId(userResource.getExternalId())
            .build();

    return asUserResource(userRepository.updateUser(id, updateUser));
  }

  @Delete("/{id}")
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteUser(@Param("id") String id) {
    User user = userRepository.getUser(id);
    checkNotMetastoreOwner(user);
    authorizer.clearAuthorizationsForPrincipal(
        UUID.fromString(Objects.requireNonNull(user.getId())));
    userRepository.deleteUser(user.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  @Patch("/{id}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse patchUser(@Param("id") String id, PatchRequest patchRequest) {
    // Only a replace of "active" is supported, as path "active" (also with the core User schema
    // URN) or as the pathless {"active": ...} object Okta sends. Any other operation, path or
    // attribute is refused as a whole, so no part of the request is applied.
    List<PatchOperation> operations = patchRequest.getOperations();
    if (operations.isEmpty() || !operations.stream().allMatch(this::isActiveReplace)) {
      return HttpResponse.of(HttpStatus.NOT_IMPLEMENTED);
    }
    // Every value is validated before anything is applied; the last operation wins.
    List<Boolean> values = operations.stream().map(op -> parseActive(activeValue(op))).toList();
    boolean active = values.get(values.size() - 1);
    if (!active) {
      checkNotMetastoreOwner(userRepository.getUser(id));
    }
    userRepository.updateUser(id, UpdateUser.builder().active(active).build());
    return HttpResponse.of(HttpStatus.OK);
  }

  /**
   * The single name UC stores for a SCIM user: displayName, else name.formatted, else givenName and
   * familyName joined by a space. Blank values are skipped; empty if none is left.
   */
  private static Optional<String> userName(UserResource user) {
    Name name = user.getName();
    String formatted = name == null ? null : name.getFormatted();
    String givenAndFamily =
        name == null
            ? null
            : Stream.of(name.getGivenName(), name.getFamilyName())
                .filter(Scim2UserService::isNotBlank)
                .collect(Collectors.joining(" "));
    return Stream.of(user.getDisplayName(), formatted, givenAndFamily)
        .filter(Scim2UserService::isNotBlank)
        .findFirst();
  }

  private static boolean isNotBlank(String value) {
    return value != null && !value.isBlank();
  }

  private boolean isActiveReplace(PatchOperation operation) {
    return operation.getOpType() == PatchOpType.REPLACE && activeValue(operation) != null;
  }

  /** The value a replace sets "active" to, or null if the operation touches anything else. */
  private JsonNode activeValue(PatchOperation operation) {
    JsonNode value = operation.getJsonNode();
    if (operation.getPath() != null) {
      String path = operation.getPath().toString();
      if (path.regionMatches(true, 0, CORE_USER_SCHEMA, 0, CORE_USER_SCHEMA.length())) {
        path = path.substring(CORE_USER_SCHEMA.length());
      }
      boolean isActivePath = ACTIVE.equalsIgnoreCase(path);
      return isActivePath ? unwrapSingleValue(value) : null;
    }
    boolean isOnlyActive =
        value.isObject() && value.size() == 1 && ACTIVE.equalsIgnoreCase(value.fieldNames().next());
    return isOnlyActive ? unwrapSingleValue(value.elements().next()) : null;
  }

  private static JsonNode unwrapSingleValue(JsonNode value) {
    return value.isArray() && value.size() == 1 ? value.get(0) : value;
  }

  private boolean parseActive(JsonNode value) {
    if (value.isBoolean()) {
      return value.booleanValue();
    }
    if (value.isTextual() && "true".equalsIgnoreCase(value.textValue())) {
      return true;
    }
    if (value.isTextual() && "false".equalsIgnoreCase(value.textValue())) {
      return false;
    }
    throw new Scim2RuntimeException(new BadRequestException("active must be true or false."));
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
