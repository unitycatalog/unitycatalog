package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.ModelVersionStatus;
import jakarta.persistence.*;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

// Hibernate annotations
@Entity
@Table(
    name = "uc_model_versions",
    indexes = {
      @Index(name = "idx_model_version", columnList = "registered_model_id,version"),
    })
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class ModelVersionInfoDAO {
  @Id
  @Column(name = "id")
  private UUID id;

  @Column(name = "registered_model_id")
  private UUID registeredModelId;

  @Column(name = "version")
  private Long version;

  @Column(name = "source")
  private String source;

  @Column(name = "run_id")
  private String runId;

  @Column(name = "status")
  private String status;

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

  @Column(name = "comment", length = 65535)
  private String comment;

  @Column(name = "url", length = 4096)
  private String url;

  public static ModelVersionInfoDAO from(ModelVersionInfo modelVersionInfo) {
    return ModelVersionInfoDAO.builder()
        .id(UUID.fromString(modelVersionInfo.getId()))
        .runId(modelVersionInfo.getRunId())
        .source(modelVersionInfo.getSource())
        .status(modelVersionInfo.getStatus().getValue())
        .comment(modelVersionInfo.getComment())
        .version(modelVersionInfo.getVersion())
        .createdAt(
            modelVersionInfo.getCreatedAt() != null
                ? new Date(modelVersionInfo.getCreatedAt())
                : new Date())
        .createdBy(modelVersionInfo.getCreatedBy())
        .updatedAt(
            modelVersionInfo.getUpdatedAt() != null
                ? new Date(modelVersionInfo.getUpdatedAt())
                : null)
        .updatedBy(modelVersionInfo.getUpdatedBy())
        .url(modelVersionInfo.getStorageLocation())
        .build();
  }

  public ModelVersionInfo toModelVersionInfo() {
    ModelVersionInfo modelVersionInfo =
        new ModelVersionInfo()
            .id(getId().toString())
            .runId(getRunId())
            .source(getSource())
            .version(getVersion())
            .status(ModelVersionStatus.valueOf(getStatus()))
            .storageLocation(url)
            .comment(comment)
            .createdAt(createdAt != null ? createdAt.getTime() : null)
            .createdBy(createdBy)
            .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
            .updatedBy(updatedBy);
    return modelVersionInfo;
  }
}
