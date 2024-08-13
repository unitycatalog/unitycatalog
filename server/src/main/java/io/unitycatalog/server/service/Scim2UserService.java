package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import com.unboundid.scim2.common.exceptions.BadRequestException;
import com.unboundid.scim2.common.exceptions.PreconditionFailedException;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.exceptions.ScimException;
import com.unboundid.scim2.common.filters.Filter;
import com.unboundid.scim2.common.types.Email;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.types.UserResource;
import com.unboundid.scim2.common.utils.FilterEvaluator;
import com.unboundid.scim2.common.utils.Parser;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.model.CreateUser;
import io.unitycatalog.server.model.UpdateUser;
import io.unitycatalog.server.model.User;
import io.unitycatalog.server.persist.UserRepository;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@ExceptionHandler(GlobalExceptionHandler.class)
public class Scim2UserService {
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();
  private final UnityCatalogAuthorizer authorizer;

  public Scim2UserService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
  }

  @Get("")
  public HttpResponse getScimUsers(
      @Param("filter") Optional<String> filter,
      @Param("startIndex") Optional<Integer> startIndex,
      @Param("count") Optional<Integer> count) {
    final Filter userFilter = filter.filter(f -> !f.isEmpty()).map(this::parseFilter).orElse(null);
    FilterEvaluator filterEvaluator = new FilterEvaluator();

    List<UserResource> userResourcesList =
        USER_REPOSITORY.listUsers().stream()
            .map(this::asUserResource)
            .filter(m -> match(filterEvaluator, userFilter, m))
            .skip(startIndex.orElse(1) - 1)
            .limit(count.orElse(50))
            .toList();
    return HttpResponse.ofJson(userResourcesList);
  }

  @Post("")
  public HttpResponse createScimUser(UserResource userResource) {
    // Get primary email address
    Email primaryEmail =
        userResource.getEmails().stream()
            .filter(email -> email.getPrimary())
            .findFirst()
            .orElseThrow(
                () ->
                    new Scim2RuntimeException(
                        new PreconditionFailedException("User does not have a primary email.")));

    try {
      User user = USER_REPOSITORY.getUserByEmail(primaryEmail.getValue());
      return HttpResponse.ofJson(asUserResource(user));
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        User user =
            USER_REPOSITORY.createUser(
                new CreateUser()
                    .name(userResource.getUserName())
                    .email(primaryEmail.getValue())
                    .externalId(userResource.getExternalId()));
        return HttpResponse.ofJson(asUserResource(user));
      } else {
        throw e;
      }
    }
  }

  @Get("/{id}")
  public HttpResponse getUser(@Param("id") String id) {
    return HttpResponse.ofJson(asUserResource(USER_REPOSITORY.getUser(id)));
  }

  @Put("/{id}")
  public HttpResponse updateUser(@Param("id") String id, UserResource userResource) {
    if (!id.equals(userResource.getId())) {
      throw new Scim2RuntimeException(new ResourceConflictException("User id mismatch."));
    }
    // Get primary email address
    Email primaryEmail =
        userResource.getEmails().stream()
            .filter(email -> email.getPrimary())
            .findFirst()
            .orElseThrow(
                () ->
                    new Scim2RuntimeException(
                        new PreconditionFailedException("User does not have a primary email.")));

    UpdateUser updateUser =
        new UpdateUser()
            .newName(userResource.getUserName())
            .email(primaryEmail.getValue())
            .externalId(userResource.getExternalId());

    return HttpResponse.ofJson(asUserResource(USER_REPOSITORY.updateUser(id, updateUser)));
  }

  @Delete("/{id}")
  public HttpResponse deleteUser(@Param("id") String id) {
    User user = USER_REPOSITORY.getUser(id);
    authorizer.clearAuthorizations(UUID.fromString(Objects.requireNonNull(user.getId())));
    USER_REPOSITORY.deleteUser(user.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  public UserResource asUserResource(User user) {
    Meta meta = new Meta();
    Calendar created = Calendar.getInstance();
    if (user.getCreatedAt() != null) {
      created.setTimeInMillis(user.getCreatedAt());
    }
    meta.setCreated(created);
    Calendar lastModified = Calendar.getInstance();
    if (user.getUpdatedAt() != null) {
      lastModified.setTimeInMillis(user.getUpdatedAt());
    }
    meta.setLastModified(lastModified);
    meta.setResourceType("User");

    UserResource userResource = new UserResource();
    userResource
        .setUserName(user.getName())
        .setEmails(List.of(new Email().setValue(user.getEmail()).setPrimary(true)));
    userResource.setId(user.getId());
    userResource.setMeta(meta);
    userResource.setActive(true);
    userResource.setExternalId(user.getExternalId());

    return userResource;
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
