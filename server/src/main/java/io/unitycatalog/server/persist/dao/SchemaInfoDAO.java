package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.SchemaInfo;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;

@Entity
@Table(name = "uc_schemas")
// Lombok
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SchemaInfoDAO {
  @Id
  @Column(name = "id")
  private UUID id;

  @Column(name = "name")
  private String name;

  @Column(name = "catalog_id")
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

  public static SchemaInfo toSchemaInfo(SchemaInfoDAO schemaInfoDAO) {
    return new SchemaInfo()
        .schemaId(schemaInfoDAO.getId().toString())
        .name(schemaInfoDAO.getName())
        .comment(schemaInfoDAO.getComment())
        .createdAt(schemaInfoDAO.getCreatedAt().getTime())
        .updatedAt(
            (schemaInfoDAO.getUpdatedAt() != null) ? schemaInfoDAO.getUpdatedAt().getTime() : null);
  }
}
