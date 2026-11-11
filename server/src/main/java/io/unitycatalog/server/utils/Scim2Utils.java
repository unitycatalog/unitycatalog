package io.unitycatalog.server.utils;

import com.unboundid.scim2.common.types.Email;
import com.unboundid.scim2.common.types.GroupResource;
import com.unboundid.scim2.common.types.Member;
import com.unboundid.scim2.common.types.Meta;
import com.unboundid.scim2.common.types.Photo;
import com.unboundid.scim2.common.types.UserResource;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.persist.model.Group;
import java.net.URI;
import java.util.Calendar;
import java.util.List;

public class Scim2Utils {
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

    return userResource;
  }

  public static GroupResource asGroupResource(Group group) {
    Meta meta = new Meta();
    Calendar created = Calendar.getInstance();
    created.setTime(group.getCreatedAt());
    meta.setCreated(created);
    Calendar lastModified = Calendar.getInstance();
    lastModified.setTime(
        group.getUpdatedAt() == null ? group.getCreatedAt() : group.getUpdatedAt());
    meta.setLastModified(lastModified);
    meta.setResourceType("Group");

    GroupResource groupResource = new GroupResource();
    groupResource
        .setDisplayName(group.getName())
        .setMembers(
            group.getMemberIds().stream()
                .map(memberId -> new Member().setValue(memberId.toString()).setType("User"))
                .toList());
    groupResource.setId(group.getId().toString());
    groupResource.setMeta(meta);
    groupResource.setExternalId(group.getExternalId());
    return groupResource;
  }
}
