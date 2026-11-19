package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.S3AddressingStyle;
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
      @Index(name = "idx_credential_id", columnList = "credential_id"),
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

  // The S3 endpoint of S3-compatible storage, set exactly when the credential is
  // s3_compatible_access_key (ExternalLocationRepository#validateForCredential). The length has
  // room for any host name with its scheme and port.
  @Column(name = "endpoint", length = 512)
  private String endpoint;

  // An S3AddressingStyle value, set with the endpoint; text rather than an enum column, so that a
  // value added later needs no schema step.
  @Column(name = "addressing_style", length = 32)
  private String addressingStyle;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  /**
   * @return how requests to the endpoint name the bucket, {@code PATH} if not stored, or null
   *     without an endpoint
   */
  public S3AddressingStyle getS3AddressingStyle() {
    if (endpoint == null) {
      return null;
    }
    return addressingStyle == null
        ? S3AddressingStyle.PATH
        : S3AddressingStyle.fromValue(addressingStyle);
  }

  public ExternalLocationInfo toExternalLocationInfo(String credentialName) {
    return new ExternalLocationInfo()
        .name(getName())
        .url(getUrl())
        .comment(getComment())
        .owner(getOwner())
        .id(getId() != null ? getId().toString() : null)
        .credentialId(getCredentialId() != null ? getCredentialId().toString() : null)
        .credentialName(credentialName)
        .endpoint(getEndpoint())
        .addressingStyle(getS3AddressingStyle())
        .createdAt(getCreatedAt().getTime())
        .createdBy(getCreatedBy())
        .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
        .updatedBy(getUpdatedBy());
  }
}
