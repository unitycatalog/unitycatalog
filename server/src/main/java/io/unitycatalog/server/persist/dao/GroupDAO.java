package io.unitycatalog.server.persist.dao;

import jakarta.persistence.AttributeOverride;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/** A SCIM group; {@code name} is its SCIM displayName, unique across the server. */
@Entity
@Table(
    name = "uc_groups",
    uniqueConstraints = {
      @UniqueConstraint(name = "uc_groups_display_name_key", columnNames = "display_name")
    })
@AttributeOverride(name = "name", column = @Column(name = "display_name", nullable = false))
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class GroupDAO extends IdentifiableDAO {
  @Column(name = "external_id")
  private String externalId;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;
}
