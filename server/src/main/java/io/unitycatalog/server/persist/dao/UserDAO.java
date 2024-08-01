package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.User;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Date;
import java.util.UUID;

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

    public static UserDAO from(User user) {
        return UserDAO.builder()
                .id(UUID.fromString(user.getId()))
                .email(user.getEmail())
                .externalId(user.getExternalId())
                .state(user.getState().name())
                .createdAt(new Date(user.getCreatedAt()))
                .updatedAt(user.getUpdatedAt() == null ? null : new Date(user.getUpdatedAt()))
                .build();
    }

    public User toUser() {
        return new User()
                .id(getId().toString())
                .email(getEmail())
                .externalId(getExternalId())
                .state(User.StateEnum.fromValue(getState()))
                .createdAt(getCreatedAt().getTime())
                .updatedAt(getUpdatedAt() == null ? null : getUpdatedAt().getTime());
    }
}
