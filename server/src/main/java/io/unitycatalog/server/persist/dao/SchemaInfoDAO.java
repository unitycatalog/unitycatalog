package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.SchemaInfo;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
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
  @Column(name = "catalog_id", columnDefinition = "BINARY(16)")
  private UUID catalogId;

  @Column(name = "comment")
  private String comment;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;

  public static SchemaInfoDAO from(SchemaInfo schemaInfo) {
    return SchemaInfoDAO.builder()
        .id(schemaInfo.getSchemaId() != null ? UUID.fromString(schemaInfo.getSchemaId()) : null)
        .name(schemaInfo.getName())
        .comment(schemaInfo.getComment())
        .createdAt(
            schemaInfo.getCreatedAt() != null
                ? Date.from(Instant.ofEpochMilli(schemaInfo.getCreatedAt()))
                : new Date())
        .updatedAt(
            schemaInfo.getUpdatedAt() != null
                ? Date.from(Instant.ofEpochMilli(schemaInfo.getUpdatedAt()))
                : null)
        .build();
  }

  public SchemaInfo toSchemaInfo() {
    return new SchemaInfo()
        .schemaId(getId().toString())
        .name(getName())
        .comment(getComment())
        .createdAt(getCreatedAt().getTime())
        .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null);
  }
}
