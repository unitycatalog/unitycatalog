package io.unitycatalog.server.utils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.unboundid.scim2.common.BaseScimResource;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class Scim2Utils {
  /** The UC extension schema with a user's or group's owner (RFC 7643 section 3.3). */
  public static final String OWNER_SCHEMA = "urn:unitycatalog:scim:schemas:extension:2.0:Owner";

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
    setOwnerExtension(
        userResource, JsonNodeFactory.instance.objectNode().put("owner", user.getOwner()));

    return userResource;
  }

  /**
   * The group as {@code caller} sees it: an IdP sees only its own member list, the admin sees the
   * members of both, and each list in the owner extension.
   */
  public static GroupResource asGroupResource(Group group, String caller) {
    Meta meta = new Meta();
    Calendar created = Calendar.getInstance();
    created.setTime(group.getCreatedAt());
    meta.setCreated(created);
    Calendar lastModified = Calendar.getInstance();
    lastModified.setTime(
        group.getUpdatedAt() == null ? group.getCreatedAt() : group.getUpdatedAt());
    meta.setLastModified(lastModified);
    meta.setResourceType("Group");

    boolean isAdmin = IdentityUtils.ADMIN_OWNER.equals(caller);
    Set<UUID> members = new LinkedHashSet<>(group.getIdpMemberIds());
    if (isAdmin) {
      members.addAll(group.getAdminMemberIds());
    }
    GroupResource groupResource = new GroupResource();
    groupResource
        .setDisplayName(group.getName())
        .setMembers(
            members.stream()
                .map(memberId -> new Member().setValue(memberId.toString()).setType("User"))
                .toList());
    groupResource.setId(group.getId().toString());
    groupResource.setMeta(meta);
    groupResource.setExternalId(group.getExternalId());

    ObjectNode owner = JsonNodeFactory.instance.objectNode().put("owner", group.getOwner());
    if (isAdmin) {
      owner.set("idpMembers", sourceMembers(group.getIdpMemberIds(), group));
      owner.set("adminMembers", sourceMembers(group.getAdminMemberIds(), group));
    }
    setOwnerExtension(groupResource, owner);
    return groupResource;
  }

  private static ArrayNode sourceMembers(Set<UUID> memberIds, Group group) {
    ArrayNode members = JsonNodeFactory.instance.arrayNode();
    memberIds.forEach(
        memberId ->
            members
                .addObject()
                .put("value", memberId.toString())
                .put("display", group.getMemberEmails().get(memberId)));
    return members;
  }

  private static void setOwnerExtension(BaseScimResource resource, ObjectNode owner) {
    Set<String> schemaUrns = new LinkedHashSet<>(resource.getSchemaUrns());
    schemaUrns.add(OWNER_SCHEMA);
    resource.setSchemaUrns(schemaUrns);
    resource.getExtensionObjectNode().set(OWNER_SCHEMA, owner);
  }
}
