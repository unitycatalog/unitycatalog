package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AwsS3AccessKeyRequest;
import io.unitycatalog.server.model.AwsS3AccessKeyResponse;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.service.CredentialService;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
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

/**
 * Data Access Object for storing cloud provider credentials in Unity Catalog. The credential
 * payload is stored as a JSON blob in the {@code credential} column, with the {@code
 * credentialType} discriminator indicating how to deserialize it.
 *
 * @see CredentialService
 * @see AwsCredentialVendor
 */
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
    /** Static S3 access key id; secret is resolved from server configuration at vend time. */
    S3_ACCESS_KEY,
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
    boolean hasIamRole = createRequest.getAwsIamRole() != null;
    boolean hasS3AccessKey = createRequest.getAwsS3AccessKey() != null;
    if (hasIamRole == hasS3AccessKey) {
      // Requests are already rejected by CredentialRepository, so this only guards direct callers
      // from building a DAO with no credential payload or with two of them.
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Specify exactly one of aws_iam_role or aws_s3_access_key");
    }
    if (hasIamRole) {
      dao.setAwsIamRole(createRequest.getAwsIamRole());
    } else {
      dao.setAwsS3AccessKey(createRequest.getAwsS3AccessKey());
    }
    return dao;
  }

  public CredentialInfo toCredentialInfo(Optional<String> masterAwsIamRoleArn) {
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
        masterAwsIamRoleArn.ifPresent(awsIamRole::setUnityCatalogIamArn);
        credentialInfo.setAwsIamRole(awsIamRole);
        break;
      case S3_ACCESS_KEY:
        // Only the access key id is stored/returned; secrets live in server configuration.
        credentialInfo.setAwsS3AccessKey(parseCredential(AwsS3AccessKeyResponse.class));
        break;
      // TODO: support Azure and GCP.
      default:
        // Reachable only if a CredentialType is added without extending this switch.
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION, "Unsupported credential type: " + credentialType);
    }
    return credentialInfo;
  }

  private <T> T parseCredential(CredentialType credentialType, Class<T> clazz) {
    if (getCredentialType() != credentialType) {
      // Mismatch credential type.
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          String.format("Storage credential '%s' is not %s.", getName(), credentialType));
    }
    return parseCredential(clazz);
  }

  private <T> T parseCredential(Class<T> clazz) {
    try {
      return objectMapper.readValue(credential, clazz);
    } catch (JsonProcessingException e) {
      // The stored payload does not match its credential_type discriminator: a server-side data
      // problem, not something the caller can fix.
      throw new BaseException(
          ErrorCode.INTERNAL, "Failed to parse credential of " + clazz.getSimpleName(), e);
    }
  }

  public void setAwsIamRole(AwsIamRoleRequest awsIamRole) {
    setCredential(CredentialType.AWS_IAM_ROLE, fromAwsIamRoleRequest(awsIamRole));
  }

  public void setAwsS3AccessKey(AwsS3AccessKeyRequest awsS3AccessKey) {
    setCredential(CredentialType.S3_ACCESS_KEY, fromAwsS3AccessKeyRequest(awsS3AccessKey));
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

  private static AwsS3AccessKeyResponse fromAwsS3AccessKeyRequest(
      AwsS3AccessKeyRequest awsS3AccessKeyRequest) {
    return new AwsS3AccessKeyResponse().accessKeyId(awsS3AccessKeyRequest.getAccessKeyId());
  }

  public AwsIamRoleResponse getAwsIamRoleResponse() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleResponse.class);
  }

  public AwsS3AccessKeyResponse getAwsS3AccessKeyResponse() {
    return parseCredential(CredentialType.S3_ACCESS_KEY, AwsS3AccessKeyResponse.class);
  }
}
