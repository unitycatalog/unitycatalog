package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ExternalLocationInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Entity
@Table(
    name = "uc_external_locations",
    indexes = {
      @Index(name = "idx_url", columnList = "url"),
    })
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
