package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import com.unboundid.scim2.common.exceptions.PreconditionFailedException;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.types.Email;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.types.UserResource;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateUser;
import io.unitycatalog.server.model.UpdateUser;
import io.unitycatalog.server.model.User;
import io.unitycatalog.server.persist.UserRepository;
import java.util.Calendar;
import java.util.List;

@ExceptionHandler(GlobalExceptionHandler.class)
public class Scim2UserService {
  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();

  public Scim2UserService() {}

  @Post("")
  public HttpResponse createScimUser(UserResource userResource) throws PreconditionFailedException {
    // Get primary email address
    Email primaryEmail =
        userResource.getEmails().stream()
            .filter(email -> email.getPrimary())
            .findFirst()
            .orElseThrow(
                () -> new PreconditionFailedException("User does not have a primary email."));

    try {
      User user = USER_REPOSITORY.getUser(userResource.getUserName());
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

  @Get("/{name}")
  public HttpResponse getUser(@Param("name") String name) {
    return HttpResponse.ofJson(USER_REPOSITORY.getUser(name));
  }

  @Put("/{name}")
  public HttpResponse updateUser(@Param("name") String name, UserResource userResource)
      throws ResourceConflictException, PreconditionFailedException {
    if (!name.equals(userResource.getUserName())) {
      throw new ResourceConflictException("User Name mismatch.");
    }
    // Get primary email address
    Email primaryEmail =
        userResource.getEmails().stream()
            .filter(email -> email.getPrimary())
            .findFirst()
            .orElseThrow(
                () -> new PreconditionFailedException("User does not have a primary email."));

    UpdateUser updateUser =
        new UpdateUser()
            .newName(userResource.getUserName())
            .email(primaryEmail.getValue())
            .externalId(userResource.getExternalId());

    return HttpResponse.ofJson(asUserResource(USER_REPOSITORY.updateUser(name, updateUser)));
  }

  @Delete("/{name}")
  public HttpResponse deleteUser(@Param("name") String name) {
    USER_REPOSITORY.deleteUser(name);
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
    userResource.setMeta(meta);
    userResource.setActive(true);
    userResource.setExternalId(user.getExternalId());

    return userResource;
  }
}
