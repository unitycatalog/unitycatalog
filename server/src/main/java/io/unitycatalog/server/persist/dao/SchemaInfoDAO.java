package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.SchemaInfo;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
    @Column(name = "id", columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(name = "name")
    private String name;

    @Column(name = "catalog_id", columnDefinition = "BINARY(16)")
    private UUID catalogId;

    @Column(name = "comment")
    private String comment;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "updated_at")
    private Date updatedAt;

    @OneToMany(mappedBy = "schema", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<PropertyDAO> properties;

    public static SchemaInfoDAO from(SchemaInfo schemaInfo) {
        return SchemaInfoDAO.builder()
                .id(schemaInfo.getSchemaId() != null ? UUID.fromString(schemaInfo.getSchemaId()) : null)
                .name(schemaInfo.getName())
                .comment(schemaInfo.getComment())
                .createdAt(schemaInfo.getCreatedAt() != null ? Date.from(Instant
                        .ofEpochMilli(schemaInfo.getCreatedAt())) : new Date())
                .updatedAt(schemaInfo.getUpdatedAt() != null ? Date.from(Instant
                        .ofEpochMilli(schemaInfo.getUpdatedAt())) : null)
                .properties(PropertyDAO.from(schemaInfo.getProperties()))
                .build();
    }

    public static SchemaInfo toSchemaInfo(SchemaInfoDAO schemaInfoDAO) {
        return new SchemaInfo()
                .schemaId(schemaInfoDAO.getId().toString())
                .name(schemaInfoDAO.getName())
                .comment(schemaInfoDAO.getComment())
                .properties(PropertyDAO.toMap(schemaInfoDAO.getProperties()))
                .createdAt(schemaInfoDAO.getCreatedAt().getTime())
                .updatedAt((schemaInfoDAO.getUpdatedAt() != null) ? schemaInfoDAO.getUpdatedAt().getTime() : null);
    }
}