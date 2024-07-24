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

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  public static CatalogInfoDAO from(CatalogInfo catalogInfo) {
    return CatalogInfoDAO.builder()
        .id(catalogInfo.getId() != null ? UUID.fromString(catalogInfo.getId()) : null)
        .name(catalogInfo.getName())
        .comment(catalogInfo.getComment())
        .createdAt(
            catalogInfo.getCreatedAt() != null
                ? Date.from(Instant.ofEpochMilli(catalogInfo.getCreatedAt()))
                : new Date())
        .updatedAt(
            catalogInfo.getUpdatedAt() != null
                ? Date.from(Instant.ofEpochMilli(catalogInfo.getUpdatedAt()))
                : null)
        .build();
  }

  public CatalogInfo toCatalogInfo() {
    return new CatalogInfo()
        .id(getId().toString())
        .name(getName())
        .comment(comment)
        .createdAt(createdAt.getTime())
        .updatedAt(updatedAt != null ? updatedAt.getTime() : null);
  }
}
