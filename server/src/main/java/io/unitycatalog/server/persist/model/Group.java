package io.unitycatalog.server.persist.model;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;

/**
 * A group and its two member lists: the members its IdP added and the members the admin added. A
 * user is a member if it is in either list. An admin-owned group has only the admin's list.
 */
@Value
@Builder(toBuilder = true)
public class Group {
  UUID id;
  String name;
  String externalId;
  String owner;
  Set<UUID> idpMemberIds;
  Set<UUID> adminMemberIds;
  // The email of each member, for listings; read from the users, not written.
  Map<UUID, String> memberEmails;
  Date createdAt;
  Date updatedAt;

  /** The member list the admin ({@code adminAdded}) or the IdP writes. */
  public Set<UUID> getMemberIds(boolean adminAdded) {
    return adminAdded ? adminMemberIds : idpMemberIds;
  }

  /** This group with {@code memberIds} as the admin's ({@code adminAdded}) or the IdP's list. */
  public Group withMemberIds(boolean adminAdded, Set<UUID> memberIds) {
    return adminAdded
        ? toBuilder().adminMemberIds(memberIds).build()
        : toBuilder().idpMemberIds(memberIds).build();
  }
}
