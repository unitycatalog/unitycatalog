package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.SchemaInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.Date;
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
    private UUID schemaId;

    @Column(name = "full_name")
    private String fullName;

    @Column(name = "name")
    private String name;

    @Column(name = "catalog_name")
    private String catalogName;

    @Column(name = "comment")
    private String comment;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "updated_at")
    private Date updatedAt;

    public static SchemaInfoDAO toSchemaInfoDAO(SchemaInfo schemaInfo) {
        return SchemaInfoDAO.builder()
            .schemaId(schemaInfo.getSchemaId() != null ? UUID.fromString(schemaInfo.getSchemaId()) : null)
            .fullName(schemaInfo.getFullName())
            .name(schemaInfo.getName())
            .catalogName(schemaInfo.getCatalogName())
            .comment(schemaInfo.getComment())
            .createdAt(schemaInfo.getCreatedAt() != null ? Date.from(Instant.ofEpochMilli(schemaInfo.getCreatedAt())) : new Date())
            .updatedAt(schemaInfo.getUpdatedAt() != null ? Date.from(Instant.ofEpochMilli(schemaInfo.getUpdatedAt())) : new Date())
            .build();
    }

    public static SchemaInfo toSchemaInfo(SchemaInfoDAO schemaInfoDAO) {
        return new SchemaInfo()
            .schemaId(schemaInfoDAO.getSchemaId().toString())
            .fullName(schemaInfoDAO.getFullName())
            .name(schemaInfoDAO.getName())
            .catalogName(schemaInfoDAO.getCatalogName())
            .comment(schemaInfoDAO.getComment())
            .createdAt(schemaInfoDAO.getCreatedAt().getTime())
            .updatedAt((schemaInfoDAO.getUpdatedAt() !=null) ? schemaInfoDAO.getUpdatedAt().getTime() : null);
    }
}
