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

    public static SchemaInfo toSchemaInfo(SchemaInfoDAO schemaInfoDAO) {
        return new SchemaInfo()
                .schemaId(schemaInfoDAO.getId().toString())
                .name(schemaInfoDAO.getName())
                .comment(schemaInfoDAO.getComment())
                .createdAt(schemaInfoDAO.getCreatedAt().getTime())
                .updatedAt((schemaInfoDAO.getUpdatedAt() !=null) ? schemaInfoDAO.getUpdatedAt().getTime() : null);
    }
}