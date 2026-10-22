package io.unitycatalog.server.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.unboundid.scim2.common.types.Email;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.types.Photo;
import com.unboundid.scim2.common.types.UserResource;
import com.unboundid.scim2.common.utils.JsonUtils;
import io.unitycatalog.control.model.OidcIdentity;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.util.Calendar;
import java.util.List;

public class Scim2Utils {
  public static final String OIDC_IDENTITY_SCHEMA_URN =
      "urn:unitycatalog:params:scim:schemas:extension:oidc:2.0:User";

  /** Reads the OIDC identity extension, or an empty identity when it is omitted. */
  public static OidcIdentity getOidcIdentity(UserResource userResource) {
    JsonNode identity = userResource.getExtensionObjectNode().get(OIDC_IDENTITY_SCHEMA_URN);
    if (identity == null) {
      return new OidcIdentity();
    }
    try {
      return JsonUtils.nodeToValue(identity, OidcIdentity.class);
    } catch (JsonProcessingException e) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid OIDC identity extension");
    }
  }

  public static UserResource asUserResource(User user) {
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

    String pictureUrl = user.getPictureUrl();
    if (pictureUrl == null) {
      pictureUrl = "";
    }

    UserResource userResource = new UserResource();
    userResource
        .setUserName(user.getEmail())
        .setDisplayName(user.getName())
        .setEmails(List.of(new Email().setValue(user.getEmail()).setPrimary(true)))
        .setPhotos(List.of(new Photo().setValue(URI.create(pictureUrl))));
    userResource.setId(user.getId());
    userResource.setMeta(meta);
    userResource.setActive(user.getState() == User.StateEnum.ENABLED);
    userResource.setExternalId(user.getExternalId());
    if (user.getIssuer() != null) {
      userResource
          .getExtensionObjectNode()
          .set(
              OIDC_IDENTITY_SCHEMA_URN,
              JsonUtils.valueToNode(
                  new OidcIdentity().issuer(user.getIssuer()).subject(user.getSubject())));
      userResource.getSchemaUrns().add(OIDC_IDENTITY_SCHEMA_URN);
    }

    return userResource;
  }
}
