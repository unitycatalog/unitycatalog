package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ExternalLocationInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_external_locations")
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class ExternalLocationDAO extends IdentifiableDAO {

  @Column(name = "url", nullable = false)
  private String url;

  @Column(name = "comment")
  private String comment;

  @Column(name = "owner")
  private String owner;

  @Column(name = "credential_id", nullable = false)
  private UUID credentialId;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  public static ExternalLocationDAO from(ExternalLocationInfo externalLocationInfo) {
    return ExternalLocationDAO.builder()
        .name(externalLocationInfo.getName())
        .url(externalLocationInfo.getUrl())
        .comment(externalLocationInfo.getComment())
        .owner(externalLocationInfo.getOwner())
        .credentialId(
            externalLocationInfo.getCredentialId() != null
                ? UUID.fromString(externalLocationInfo.getCredentialId())
                : null)
        .createdAt(
            externalLocationInfo.getCreatedAt() != null
                ? Date.from(Instant.ofEpochMilli(externalLocationInfo.getCreatedAt()))
                : new Date())
        .createdBy(externalLocationInfo.getCreatedBy())
        .updatedAt(
            externalLocationInfo.getUpdatedAt() != null
                ? Date.from(Instant.ofEpochMilli(externalLocationInfo.getUpdatedAt()))
                : null)
        .updatedBy(externalLocationInfo.getUpdatedBy())
        .build();
  }

  public ExternalLocationInfo toExternalLocationInfo() {
    return new ExternalLocationInfo()
        .name(getName())
        .url(getUrl())
        .comment(getComment())
        .owner(getOwner())
        .credentialId(getCredentialId() != null ? getCredentialId().toString() : null)
        .createdAt(getCreatedAt().getTime())
        .createdBy(getCreatedBy())
        .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
        .updatedBy(getUpdatedBy());
  }
}
