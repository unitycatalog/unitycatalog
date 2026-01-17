package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
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
  private static ObjectMapper objectMapper = new ObjectMapper();

  public enum CredentialType {
    AWS_IAM_ROLE,
    // Add other types as necessary
  }

  @Column(name = "credential_type", nullable = false)
  @Enumerated(EnumType.STRING)
  // No direct setter from outside
  @Setter(AccessLevel.NONE)
  private CredentialType credentialType;

  @Lob
  @Column(name = "credential", nullable = false)
  // No direct access from outside
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
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

  public static CredentialDAO from(CreateCredentialRequest createRequest, String callerId) {
    Date now = new Date();
    CredentialDAO dao =
        CredentialDAO.builder()
            .id(UUID.randomUUID())
            .name(createRequest.getName())
            .purpose(createRequest.getPurpose())
            .comment(createRequest.getComment())
            .owner(callerId)
            .createdAt(now)
            .createdBy(callerId)
            .updatedAt(now)
            .updatedBy(callerId)
            .build();
    if (createRequest.getAwsIamRole() != null) {
      dao.setAwsIamRole(createRequest.getAwsIamRole());
    } else {
      throw new IllegalArgumentException("Unknown credential type");
    }
    return dao;
  }

  public CredentialInfo toCredentialInfo(Optional<String> unityCatalogIamArn) {
    CredentialInfo credentialInfo =
        new CredentialInfo()
            .id(getId().toString())
            .name(getName())
            .purpose(purpose)
            .comment(comment)
            .owner(owner)
            .createdAt(createdAt.getTime())
            .createdBy(createdBy)
            .updatedAt(updatedAt != null ? updatedAt.getTime() : null)
            .updatedBy(updatedBy);
    switch (credentialType) {
      case AWS_IAM_ROLE:
        AwsIamRoleResponse awsIamRole = parseCredential(AwsIamRoleResponse.class);
        unityCatalogIamArn.ifPresent(awsIamRole::setUnityCatalogIamArn);
        credentialInfo.setAwsIamRole(awsIamRole);
        break;
        // TODO: support Azure and GCP.
      default:
        throw new IllegalArgumentException("Unknown credential type: " + credentialType);
    }
    return credentialInfo;
  }

  private <T> T parseCredential(Class<T> clazz) {
    try {
      return objectMapper.readValue(credential, clazz);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to parse credential of " + clazz.getSimpleName(), e);
    }
  }

  public void setAwsIamRole(AwsIamRoleRequest awsIamRole) {
    setCredential(CredentialType.AWS_IAM_ROLE, fromAwsIamRoleRequest(awsIamRole));
  }

  private <T> void setCredential(CredentialType type, T inputCredential) {
    credentialType = type;
    try {
      credential = objectMapper.writeValueAsString(inputCredential);
      // TODO: encrypt the credential
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to parse credential of " + credential.getClass().getSimpleName(), e);
    }
  }

  private static AwsIamRoleResponse fromAwsIamRoleRequest(AwsIamRoleRequest awsIamRoleRequest) {
    return new AwsIamRoleResponse()
        .roleArn(awsIamRoleRequest.getRoleArn())
        .externalId(UUID.randomUUID().toString());
  }
}
