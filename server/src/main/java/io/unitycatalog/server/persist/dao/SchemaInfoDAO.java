package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.SchemaInfo;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_schemas")
// Lombok
@Getter
@Setter
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SchemaInfoDAO extends IdentifiableDAO {
  @Column(name = "catalog_id")
  private UUID catalogId;

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

  public static SchemaInfoDAO from(SchemaInfo schemaInfo) {
    return SchemaInfoDAO.builder()
        .id(schemaInfo.getSchemaId() != null ? UUID.fromString(schemaInfo.getSchemaId()) : null)
        .name(schemaInfo.getName())
        .comment(schemaInfo.getComment())
        .owner(schemaInfo.getOwner())
        .createdAt(
            schemaInfo.getCreatedAt() != null
                ? Date.from(Instant.ofEpochMilli(schemaInfo.getCreatedAt()))
                : new Date())
        .createdBy(schemaInfo.getCreatedBy())
        .updatedAt(
            schemaInfo.getUpdatedAt() != null
                ? Date.from(Instant.ofEpochMilli(schemaInfo.getUpdatedAt()))
                : null)
        .updatedBy(schemaInfo.getUpdatedBy())
        .build();
  }

  public SchemaInfo toSchemaInfo() {
    return new SchemaInfo()
        .schemaId(getId().toString())
        .name(getName())
        .comment(getComment())
        .owner(getOwner())
        .createdAt(getCreatedAt().getTime())
        .createdBy(getCreatedBy())
        .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
        .updatedBy(getUpdatedBy());
  }
}
