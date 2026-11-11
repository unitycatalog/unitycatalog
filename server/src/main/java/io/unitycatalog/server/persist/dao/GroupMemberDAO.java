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

/** One user in one group. */
@Entity
@Table(
    name = "uc_group_members",
    indexes = {@Index(name = "uc_group_members_user_idx", columnList = "user_id")})
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

  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class Key implements Serializable {
    private UUID groupId;
    private UUID userId;
  }
}
