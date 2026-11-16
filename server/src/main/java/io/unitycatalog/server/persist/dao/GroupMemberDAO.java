package io.unitycatalog.server.persist.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One user in one of a group's two member lists: the admin's ({@code adminAdded}) or the IdP's. A
 * user in both lists has two rows.
 */
@Entity
@Table(
    name = "uc_group_members",
    indexes = {
      @Index(name = "uc_group_members_user_idx", columnList = "user_id"),
      @Index(name = "uc_group_members_group_idx", columnList = "group_id")
    })
@IdClass(GroupMemberDAO.Key.class)
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class GroupMemberDAO {
  @Id
  @Column(name = "group_id")
  private UUID groupId;

  @Id
  @Column(name = "user_id")
  private UUID userId;

  @Id
  @Column(name = "admin_added")
  private boolean adminAdded;

  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class Key implements Serializable {
    private UUID groupId;
    private UUID userId;
    private boolean adminAdded;
  }
}
