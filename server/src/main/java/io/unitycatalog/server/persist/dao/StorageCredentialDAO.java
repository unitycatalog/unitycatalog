package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureManagedIdentityResponse;
import io.unitycatalog.server.model.AzureServicePrincipal;
import io.unitycatalog.server.model.StorageCredentialInfo;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_storage_credential")
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class StorageCredentialDAO extends IdentifiableDAO {
  public static ObjectMapper objectMapper = new ObjectMapper();

  @Column(name = "credential_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private CredentialType credentialType;

  @Lob
  @Column(name = "credential", nullable = false)
  private String credential;

  @Column(name = "comment")
  private String comment;

  @Column(name = "read_only", nullable = false)
  private Boolean readOnly;

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

  @Column(name = "used_for_managed_storage", nullable = false)
  private Boolean usedForManagedStorage;

  public static StorageCredentialDAO from(StorageCredentialInfo storageCredentialInfo) {
    StorageCredentialDAOBuilder storageCredentialDAOBuilder =
        StorageCredentialDAO.builder()
            .id(
                storageCredentialInfo.getId() != null
                    ? UUID.fromString(storageCredentialInfo.getId())
                    : null)
            .name(storageCredentialInfo.getName())
            .comment(storageCredentialInfo.getComment())
            .readOnly(storageCredentialInfo.getReadOnly())
            .owner(storageCredentialInfo.getOwner())
            .createdAt(
                storageCredentialInfo.getCreatedAt() != null
                    ? Date.from(Instant.ofEpochMilli(storageCredentialInfo.getCreatedAt()))
                    : new Date())
            .createdBy(storageCredentialInfo.getCreatedBy())
            .updatedAt(
                storageCredentialInfo.getUpdatedAt() != null
                    ? Date.from(Instant.ofEpochMilli(storageCredentialInfo.getUpdatedAt()))
                    : null)
            .updatedBy(storageCredentialInfo.getUpdatedBy())
            .usedForManagedStorage(storageCredentialInfo.getUsedForManagedStorage());
    // TODO: encrypt the credential
    try {
      if (storageCredentialInfo.getAwsIamRole() != null) {
        storageCredentialDAOBuilder.credentialType(CredentialType.AWS_IAM_ROLE);
        storageCredentialDAOBuilder.credential(
            objectMapper.writeValueAsString(storageCredentialInfo.getAwsIamRole()));
      } else if (storageCredentialInfo.getAzureManagedIdentity() != null) {
        storageCredentialDAOBuilder.credentialType(CredentialType.AZURE_MANAGED_IDENTITY);
        storageCredentialDAOBuilder.credential(
            objectMapper.writeValueAsString(storageCredentialInfo.getAzureManagedIdentity()));
      } else if (storageCredentialInfo.getAzureServicePrincipal() != null) {
        storageCredentialDAOBuilder.credentialType(CredentialType.AZURE_SERVICE_PRINCIPAL);
        storageCredentialDAOBuilder.credential(
            objectMapper.writeValueAsString(storageCredentialInfo.getAzureServicePrincipal()));
      } else {
        throw new IllegalArgumentException("Unknown credential type");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize credential", e);
    }
    return storageCredentialDAOBuilder.build();
  }

  public StorageCredentialInfo toStorageCredentialInfo() {
    StorageCredentialInfo storageCredentialInfo =
        new StorageCredentialInfo()
            .id(getId().toString())
            .name(getName())
            .comment(getComment())
            .readOnly(getReadOnly())
            .owner(getOwner())
            .createdAt(getCreatedAt().getTime())
            .createdBy(getCreatedBy())
            .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
            .updatedBy(getUpdatedBy())
            .usedForManagedStorage(getUsedForManagedStorage());
    // TODO: decrypt the credential
    try {
      switch (getCredentialType()) {
        case AWS_IAM_ROLE:
          storageCredentialInfo.setAwsIamRole(
              objectMapper.readValue(getCredential(), AwsIamRoleResponse.class));
          break;
        case AZURE_MANAGED_IDENTITY:
          storageCredentialInfo.setAzureManagedIdentity(
              objectMapper.readValue(getCredential(), AzureManagedIdentityResponse.class));
          break;
        case AZURE_SERVICE_PRINCIPAL:
          storageCredentialInfo.setAzureServicePrincipal(
              objectMapper.readValue(getCredential(), AzureServicePrincipal.class));
          break;
        default:
          throw new IllegalArgumentException("Unknown credential type");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse credential", e);
    }
    return storageCredentialInfo;
  }

  public enum CredentialType {
    AWS_IAM_ROLE,
    AZURE_MANAGED_IDENTITY,
    AZURE_SERVICE_PRINCIPAL,
    // TODO: Add other types as necessary
  }
}
