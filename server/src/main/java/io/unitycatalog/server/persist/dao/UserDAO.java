package io.unitycatalog.server.persist.dao;

import io.unitycatalog.control.model.User;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_users")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class UserDAO extends IdentifiableDAO {
  @Column(name = "email")
  private String email;

  @Column(name = "external_id")
  private String externalId;

  @Column(name = "state")
  private String state;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "picture_url")
  private String pictureUrl;

  public static UserDAO from(User user) {
    return UserDAO.builder()
        .id(UUID.fromString(user.getId()))
        .name(user.getName())
        .email(user.getEmail())
        .externalId(user.getExternalId())
        .state(user.getState().name())
        .pictureUrl(user.getPictureUrl())
        .createdAt(new Date(user.getCreatedAt()))
        .updatedAt(user.getUpdatedAt() == null ? null : new Date(user.getUpdatedAt()))
        .build();
  }

  public User toUser() {
    return new User()
        .id(getId().toString())
        .name(getName())
        .email(getEmail())
        .externalId(getExternalId())
        .state(User.StateEnum.fromValue(getState()))
        .pictureUrl(getPictureUrl())
        .createdAt(getCreatedAt().getTime())
        .updatedAt(getUpdatedAt() == null ? null : getUpdatedAt().getTime());
  }
}
