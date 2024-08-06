package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.Authorization;
import io.unitycatalog.server.model.PrincipalType;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.ResourceType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_authorizations")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@SuperBuilder
public class AuthorizationDAO {
  @Id
  @Column(name = "id", updatable = false, nullable = false)
  private UUID id;

  @Column(name = "principal_id", nullable = false)
  private UUID principalId;

  @Column(name = "principal_type")
  private String principalType;

  @Column(name = "privilege")
  private String privilege;

  @Column(name = "resource_id", nullable = false)
  private UUID resourceId;

  @Column(name = "resource_type")
  private String resourceType;

  @Column(name = "created_at")
  private Date createdAt;

  public static AuthorizationDAO from(Authorization authorization) {
    return AuthorizationDAO.builder()
        .id(UUID.fromString(authorization.getId()))
        .principalId(UUID.fromString(authorization.getPrincipalId()))
        .principalType(authorization.getPrincipalType().toString())
        .privilege(authorization.getPrivilege().toString())
        .resourceId(UUID.fromString(authorization.getResourceId()))
        .resourceType(authorization.getResourceType().toString())
        .createdAt(new Date(authorization.getCreatedAt()))
        .build();
  }

  public Authorization toAuthorization() {
    return new Authorization()
        .id(getId().toString())
        .principalId(getPrincipalId().toString())
        .principalType(PrincipalType.fromValue(getPrincipalType()))
        .privilege(Privilege.fromValue(getPrivilege()))
        .resourceId(getResourceId().toString())
        .resourceType(ResourceType.fromValue(getResourceType()))
        .createdAt(getCreatedAt().getTime());
  }
}
