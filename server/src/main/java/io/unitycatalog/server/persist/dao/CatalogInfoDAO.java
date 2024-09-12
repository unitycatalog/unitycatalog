package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.CatalogInfo;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_catalogs")
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CatalogInfoDAO extends IdentifiableDAO {
  @Column(name = "comment")
  private String comment;

  @Column(name = "owner")
  private String owner;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  public static CatalogInfoDAO from(CatalogInfo catalogInfo) {
    return CatalogInfoDAO.builder()
        .id(catalogInfo.getId() != null ? UUID.fromString(catalogInfo.getId()) : null)
        .name(catalogInfo.getName())
        .comment(catalogInfo.getComment())
        .owner(catalogInfo.getOwner())
        .createdAt(
            catalogInfo.getCreatedAt() != null
                ? Date.from(Instant.ofEpochMilli(catalogInfo.getCreatedAt()))
                : new Date())
        .createdBy(catalogInfo.getCreatedBy())
        .updatedAt(
            catalogInfo.getUpdatedAt() != null
                ? Date.from(Instant.ofEpochMilli(catalogInfo.getUpdatedAt()))
                : null)
        .updatedBy(catalogInfo.getUpdatedBy())
        .build();
  }

  public CatalogInfo toCatalogInfo() {
    return new CatalogInfo()
        .id(getId().toString())
        .name(getName())
        .comment(comment)
        .owner(owner)
        .createdAt(createdAt.getTime())
        .createdBy(createdBy)
        .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
        .updatedBy(updatedBy);
  }
}
