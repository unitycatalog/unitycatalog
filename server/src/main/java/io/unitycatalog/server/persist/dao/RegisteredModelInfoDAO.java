package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.utils.FileUtils;
import jakarta.persistence.*;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(
        name = "uc_registered_models",
        indexes = {
                @Index(name = "idx_name", columnList = "name"),
        })
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class RegisteredModelInfoDAO extends IdentifiableDAO {
    @Column(name = "schema_id")
    private UUID schemaId;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "created_by")
    private String createdBy;

    @Column(name = "updated_at")
    private Date updatedAt;

    @Column(name = "updated_by")
    private String updatedBy;

    @Column(name = "comment", length = 65535)
    private String comment;

    @Column(name = "url", length = 2048)
    private String url;

    public static RegisteredModelInfoDAO from(RegisteredModelInfo registeredModelInfo) {
        return RegisteredModelInfoDAO.builder()
                .id(UUID.fromString(registeredModelInfo.getModelId()))
                .name(registeredModelInfo.getName())
                .comment(registeredModelInfo.getComment())
                .createdAt(
                        registeredModelInfo.getCreatedAt() != null ? new Date(registeredModelInfo.getCreatedAt()) : new Date())
                .createdBy(registeredModelInfo.getCretedBy())
                .updatedAt(registeredModelInfo.getUpdatedAt() != null ? new Date(registeredModelInfo.getUpdatedAt()) : new Date())
                .createdBy(registeredModelInfo.getUpdatedBy())
                .url(registeredModelInfo.getStorageLocation())
                .build();
    }

    public RegisteredModelInfo toRegisteredModelInfo() {
        RegisteredModelInfo registeredModelInfo =
                new RegisteredModelInfo()
                        .modelId(getId().toString())
                        .name(getName())
                        .storageLocation(FileUtils.convertRelativePathToURI(url))
                        .comment(comment)
                        .createdAt(createdAt != null ? createdAt.getTime() : null)
                        .updatedAt(updatedAt != null ? updatedAt.getTime() : null);
        return registeredModelInfo;
    }
}
