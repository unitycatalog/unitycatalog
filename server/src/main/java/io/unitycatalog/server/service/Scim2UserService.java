package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.utils.Scim2Utils.asUserResource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
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
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.UpdateUser;
import io.unitycatalog.server.utils.Scim2Utils;
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
 */
@ExceptionHandler(GlobalExceptionHandler.class)
public class Scim2UserService {
  private final UserRepository userRepository;
  private final UnityCatalogAuthorizer authorizer;

  public Scim2UserService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.userRepository = repositories.getUserRepository();
  }

  @Get("")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression("#principal != null")
  @AuthorizeKey(METASTORE)
  public ListResponse<UserResource> getScimUsers(
      @Param("filter") Optional<String> filter,
      @Param("startIndex") Optional<Integer> startIndex,
      @Param("count") Optional<Integer> count) {
    final Filter userFilter = filter.filter(f -> !f.isEmpty()).map(this::parseFilter).orElse(null);
    FilterEvaluator filterEvaluator = new FilterEvaluator();

    List<UserResource> userResourcesList =
        userRepository
            .listUsers(
                startIndex.orElse(1) - 1,
                count.orElse(50),
                m -> match(filterEvaluator, userFilter, asUserResource(m)))
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
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
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
    try {
      User user =
          userRepository.createUser(
              CreateUser.builder()
                  .name(userResource.getDisplayName())
                  .email(primaryEmail.getValue())
                  .active(userResource.getActive())
                  .externalId(userResource.getExternalId())
                  .pictureUrl(pictureUrl)
                  .build());
      return asUserResource(user);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.ALREADY_EXISTS) {
        throw new Scim2RuntimeException(new ResourceConflictException(e.getMessage()));
      } else {
        throw new Scim2RuntimeException(new BadRequestException(e.getMessage()));
      }
    }
  }

  @Get("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression("#principal != null")
  @AuthorizeKey(METASTORE)
  public UserResource getUser(@Param("id") String id) {
    return asUserResource(userRepository.getUser(id));
  }

  @Put("/{id}")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public UserResource updateUser(@Param("id") String id, UserResource userResource) {
    UserResource user = asUserResource(userRepository.getUser(id));
    if (!id.equals(userResource.getId())) {
      throw new Scim2RuntimeException(new ResourceConflictException("User id mismatch."));
    }

    UpdateUser updateUser =
        UpdateUser.builder()
            .name(userResource.getDisplayName())
            .active(userResource.getActive())
            .externalId(userResource.getExternalId())
            .build();

    return asUserResource(userRepository.updateUser(id, updateUser));
  }

  @Delete("/{id}")
  @AuthorizeExpression("#authorizeAny(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteUser(@Param("id") String id) {
    User user = userRepository.getUser(id);
    authorizer.clearAuthorizationsForPrincipal(
        UUID.fromString(Objects.requireNonNull(user.getId())));
    userRepository.deleteUser(user.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  @Patch("/{id}")
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
      Boolean value = operation.getValues(Boolean.class).get(0);
      UpdateUser updateUser = UpdateUser.builder().active(value).build();
      userRepository.updateUser(id, updateUser);
      return HttpResponse.of(HttpStatus.OK);
    } catch (ScimException | JsonProcessingException e) {
      return handleExceptionDuringPatch(e);
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
