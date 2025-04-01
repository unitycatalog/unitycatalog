package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.utils.EncryptionUtils;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_credentials")
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CredentialDAO extends IdentifiableDAO {
  public static ObjectMapper objectMapper = new ObjectMapper();

  public enum CredentialType {
    AWS_IAM_ROLE,
    // Add other types as necessary
  }

  @Column(name = "credential_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private CredentialType credentialType;

  @Lob
  @Column(name = "credential", nullable = false)
  private String credential;

  @Column(name = "purpose", nullable = false)
  private CredentialPurpose purpose;

  @Column(name = "comment")
  private String comment;

  @Column(name = "owner")
  private String owner;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  public static CredentialDAO from(CredentialInfo credentialInfo) {
    CredentialDAOBuilder credentialDAOBuilder =
        CredentialDAO.builder()
            .id(credentialInfo.getId() != null ? UUID.fromString(credentialInfo.getId()) : null)
            .name(credentialInfo.getName())
            .purpose(credentialInfo.getPurpose())
            .comment(credentialInfo.getComment())
            .owner(credentialInfo.getOwner())
            .createdAt(
                credentialInfo.getCreatedAt() != null
                    ? Date.from(Instant.ofEpochMilli(credentialInfo.getCreatedAt()))
                    : new Date())
            .createdBy(credentialInfo.getCreatedBy())
            .updatedAt(
                credentialInfo.getUpdatedAt() != null
                    ? Date.from(Instant.ofEpochMilli(credentialInfo.getUpdatedAt()))
                    : null)
            .updatedBy(credentialInfo.getUpdatedBy());
    try {
      if (credentialInfo.getAwsIamRole() != null) {
        credentialDAOBuilder.credentialType(CredentialType.AWS_IAM_ROLE);
        String jsonCredential = objectMapper.writeValueAsString(credentialInfo.getAwsIamRole());
        // Encrypt the credential before storing
        String encryptedCredential = EncryptionUtils.encrypt(jsonCredential);
        credentialDAOBuilder.credential(encryptedCredential);
      } else {
        throw new IllegalArgumentException("Unknown credential type");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize credential", e);
    }
    return credentialDAOBuilder.build();
  }

  public CredentialInfo toCredentialInfo() {
    CredentialInfo credentialInfo =
        new CredentialInfo()
            .id(getId().toString())
            .name(getName())
            .purpose(getPurpose())
            .comment(getComment())
            .owner(getOwner())
            .createdAt(getCreatedAt().getTime())
            .createdBy(getCreatedBy())
            .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
            .updatedBy(getUpdatedBy());
    try {
      // Decrypt the credential before using
      String decryptedCredential = EncryptionUtils.decrypt(getCredential());

      switch (getCredentialType()) {
        case AWS_IAM_ROLE:
          credentialInfo.setAwsIamRole(
              objectMapper.readValue(decryptedCredential, AwsIamRoleResponse.class));
          break;
        default:
          throw new IllegalArgumentException("Unknown credential type");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse credential", e);
    }
    return credentialInfo;
  }
}
