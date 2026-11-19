package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleAccessKeyRequest;
import io.unitycatalog.server.model.S3CompatibleAccessKeyResponse;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.persist.secret.CredentialSecretStore.SecretContext;
import io.unitycatalog.server.service.CredentialService;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.utils.ValidationUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HexFormat;
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
 * payload is stored as JSON text in the {@code credential} column, with the {@code credentialType}
 * discriminator indicating how to deserialize it.
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
  // Write-only request field of s3_compatible_access_key: stored in the JSON next to the
  // S3CompatibleAccessKeyResponse fields and dropped when the JSON is read back as a response.
  private static final String SECRET_ACCESS_KEY_FIELD = "secret_access_key";

  public enum CredentialType {
    AWS_IAM_ROLE,
    S3_COMPATIBLE,
    // Add other types as necessary
  }

  @Column(name = "credential_type", nullable = false)
  @Enumerated(EnumType.STRING)
  // No direct setter from outside
  @Setter(AccessLevel.NONE)
  private CredentialType credentialType;

  // Text, not a large object; the length makes it mediumtext rather than tinytext on MySQL.
  @Column(name = "credential", nullable = false, length = 16777215)
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

  /**
   * @param createRequest the request to create the credential from
   * @param callerId the principal creating it
   * @param secretStore keeps the credential's secret values
   * @return the new credential
   */
  public static CredentialDAO from(
      CreateCredentialRequest createRequest, String callerId, CredentialSecretStore secretStore) {
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
    ValidationUtils.checkArgument(
        createRequest.getAwsIamRole() == null || createRequest.getS3CompatibleAccessKey() == null,
        "Specify only one of aws_iam_role and s3_compatible_access_key.");
    if (createRequest.getAwsIamRole() != null) {
      dao.setAwsIamRole(createRequest.getAwsIamRole());
    } else if (createRequest.getS3CompatibleAccessKey() != null) {
      dao.setS3CompatibleAccessKey(createRequest.getS3CompatibleAccessKey(), secretStore);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Specify exactly one of aws_iam_role and s3_compatible_access_key.");
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
        AwsIamRoleResponse awsIamRole = getAwsIamRoleResponse();
        masterAwsIamRoleArn.ifPresent(awsIamRole::setUnityCatalogIamArn);
        credentialInfo.setAwsIamRole(awsIamRole);
        break;
      case S3_COMPATIBLE:
        credentialInfo.setS3CompatibleAccessKey(getS3CompatibleAccessKeyResponse());
        break;
      // TODO: support Azure and GCP.
      default:
        throw new IllegalArgumentException("Unknown credential type: " + credentialType);
    }
    return credentialInfo;
  }

  private ObjectNode readCredential(CredentialType expectedType) {
    if (getCredentialType() != expectedType) {
      // Mismatch credential type.
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          String.format("Storage credential '%s' is not %s.", getName(), expectedType));
    }
    // A stored value that does not parse is the server's fault, not the caller's.
    JsonNode stored;
    try {
      stored = objectMapper.readTree(credential);
    } catch (JsonProcessingException e) {
      throw unreadable(expectedType, e);
    }
    if (!(stored instanceof ObjectNode object)) {
      throw unreadable(expectedType, null);
    }
    return object;
  }

  private BaseException unreadable(CredentialType expectedType, Throwable cause) {
    return new BaseException(
        ErrorCode.INTERNAL,
        String.format("Stored credential '%s' of type %s cannot be read.", getName(), expectedType),
        cause);
  }

  /** Parses the stored credential as its API response, without the write-only field. */
  private <T> T parseCredential(CredentialType expectedType, Class<T> clazz) {
    ObjectNode stored = readCredential(expectedType);
    stored.remove(SECRET_ACCESS_KEY_FIELD);
    try {
      return objectMapper.treeToValue(stored, clazz);
    } catch (JsonProcessingException e) {
      throw unreadable(expectedType, e);
    }
  }

  public void setAwsIamRole(AwsIamRoleRequest awsIamRole) {
    setCredential(CredentialType.AWS_IAM_ROLE, fromAwsIamRoleRequest(awsIamRole));
  }

  /**
   * Validates an {@code s3_compatible_access_key} credential and stores it, replacing the stored
   * credential; its {@code secret_access_key} goes through {@code secretStore}, bound to this
   * credential's id.
   *
   * @param accessKey the credential to store
   * @param secretStore keeps its {@code secret_access_key}
   */
  public void setS3CompatibleAccessKey(
      S3CompatibleAccessKeyRequest accessKey, CredentialSecretStore secretStore) {
    validateS3CompatibleAccessKey(accessKey);
    ObjectNode stored = objectMapper.valueToTree(fromS3CompatibleAccessKeyRequest(accessKey));
    stored.put(
        SECRET_ACCESS_KEY_FIELD,
        secretStore.store(accessKey.getSecretAccessKey(), getS3SecretAccessKeyContext()));
    setCredential(CredentialType.S3_COMPATIBLE, stored);
  }

  private static void validateS3CompatibleAccessKey(S3CompatibleAccessKeyRequest accessKey) {
    ValidationUtils.checkArgument(
        accessKey.getAccessKeyId() != null && !accessKey.getAccessKeyId().isEmpty(),
        "s3_compatible_access_key.access_key_id is required.");
    ValidationUtils.checkArgument(
        accessKey.getSecretAccessKey() != null && !accessKey.getSecretAccessKey().isEmpty(),
        "s3_compatible_access_key.secret_access_key is required.");
    S3AssumeRole assumeRole = accessKey.getAssumeRole();
    if (assumeRole != null) {
      if (assumeRole.getStsEndpoint() != null) {
        ValidationUtils.validateHttpUrl(
            assumeRole.getStsEndpoint(), "s3_compatible_access_key.assume_role.sts_endpoint");
      }
      ValidationUtils.checkArgument(
          assumeRole.getRoleArn() != null && !assumeRole.getRoleArn().isEmpty(),
          "s3_compatible_access_key.assume_role.role_arn is required.");
      // A region name signs AssumeRole requests; storage may use its own names, such as "garage".
      ValidationUtils.checkArgument(
          assumeRole.getStsRegion() == null || assumeRole.getStsRegion().matches("[A-Za-z0-9._-]+"),
          "Invalid s3_compatible_access_key.assume_role.sts_region: use letters, digits, '.', '_'"
              + " and '-'.");
    }
  }

  private <T> void setCredential(CredentialType type, T inputCredential) {
    credentialType = type;
    try {
      credential = objectMapper.writeValueAsString(inputCredential);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to parse credential of " + inputCredential.getClass().getSimpleName(), e);
    }
  }

  private static AwsIamRoleResponse fromAwsIamRoleRequest(AwsIamRoleRequest awsIamRoleRequest) {
    return new AwsIamRoleResponse()
        .roleArn(awsIamRoleRequest.getRoleArn())
        .externalId(UUID.randomUUID().toString());
  }

  private static S3CompatibleAccessKeyResponse fromS3CompatibleAccessKeyRequest(
      S3CompatibleAccessKeyRequest accessKey) {
    // An sts_endpoint not set stays absent, and AssumeRole goes to the endpoint of the requested
    // path's external location at vend time.
    return new S3CompatibleAccessKeyResponse()
        .accessKeyId(accessKey.getAccessKeyId())
        .assumeRole(accessKey.getAssumeRole());
  }

  public AwsIamRoleResponse getAwsIamRoleResponse() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleResponse.class);
  }

  public S3CompatibleAccessKeyResponse getS3CompatibleAccessKeyResponse() {
    return parseCredential(CredentialType.S3_COMPATIBLE, S3CompatibleAccessKeyResponse.class);
  }

  /**
   * @param secretStore the store that keeps it
   * @return the write-only {@code secret_access_key} of an {@code s3_compatible_access_key}
   *     credential
   */
  public String getS3SecretAccessKey(CredentialSecretStore secretStore) {
    JsonNode stored = readCredential(CredentialType.S3_COMPATIBLE).get(SECRET_ACCESS_KEY_FIELD);
    if (stored == null || !stored.isTextual()) {
      throw new BaseException(
          ErrorCode.INTERNAL,
          String.format("Storage credential '%s' has no stored secret_access_key.", getName()));
    }
    return secretStore.load(stored.asText(), getS3SecretAccessKeyContext());
  }

  /**
   * @return a fingerprint of the stored credential: any change of its payload changes it, while
   *     changes of other fields, such as the comment, keep it
   */
  public String getCredentialFingerprint() {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(credential.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** What the {@code secret_access_key} belongs to, for its store. */
  public SecretContext getS3SecretAccessKeyContext() {
    return new SecretContext(getId(), getName(), SECRET_ACCESS_KEY_FIELD);
  }
}
