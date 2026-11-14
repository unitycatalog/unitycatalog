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
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.S3CompatibleResponse;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.persist.secret.CredentialSecretStore.SecretContext;
import io.unitycatalog.server.service.CredentialService;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.utils.ValidationUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
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
  // Write-only request field of s3_compatible: stored in the blob next to the S3CompatibleResponse
  // fields and dropped when the blob is read back as a response.
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
        createRequest.getAwsIamRole() == null || createRequest.getS3Compatible() == null,
        "Specify only one of aws_iam_role and s3_compatible.");
    if (createRequest.getAwsIamRole() != null) {
      dao.setAwsIamRole(createRequest.getAwsIamRole());
    } else if (createRequest.getS3Compatible() != null) {
      dao.setS3Compatible(createRequest.getS3Compatible(), secretStore);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Specify exactly one of aws_iam_role and s3_compatible.");
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
        credentialInfo.setS3Compatible(getS3CompatibleResponse());
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
    JsonNode stored;
    try {
      stored = objectMapper.readTree(credential);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse credential of " + expectedType, e);
    }
    if (!(stored instanceof ObjectNode object)) {
      throw new IllegalArgumentException("Failed to parse credential of " + expectedType);
    }
    return object;
  }

  /** Parses the stored credential as its API response, without the write-only field. */
  private <T> T parseCredential(CredentialType expectedType, Class<T> clazz) {
    ObjectNode stored = readCredential(expectedType);
    stored.remove(SECRET_ACCESS_KEY_FIELD);
    try {
      return objectMapper.treeToValue(stored, clazz);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to parse credential of " + clazz.getSimpleName(), e);
    }
  }

  public void setAwsIamRole(AwsIamRoleRequest awsIamRole) {
    setCredential(CredentialType.AWS_IAM_ROLE, fromAwsIamRoleRequest(awsIamRole));
  }

  /**
   * Validates an {@code s3_compatible} credential and stores it, replacing the stored credential;
   * its {@code secret_access_key} goes through {@code secretStore}, bound to this credential's id.
   *
   * <p>TODO(#2169): on MySQL, {@code credential} is {@code tinytext} (255 bytes) until #2169 widens
   * it, which an {@code s3_compatible} credential with {@code assume_role} and real endpoints
   * usually exceeds.
   *
   * @param s3Compatible the credential to store
   * @param secretStore keeps its {@code secret_access_key}
   */
  public void setS3Compatible(S3CompatibleRequest s3Compatible, CredentialSecretStore secretStore) {
    validateS3Compatible(s3Compatible);
    ObjectNode stored = objectMapper.valueToTree(fromS3CompatibleRequest(s3Compatible));
    stored.put(
        SECRET_ACCESS_KEY_FIELD,
        secretStore.store(s3Compatible.getSecretAccessKey(), getS3SecretAccessKeyContext()));
    setCredential(CredentialType.S3_COMPATIBLE, stored);
  }

  private static void validateS3Compatible(S3CompatibleRequest s3Compatible) {
    ValidationUtils.checkArgument(
        s3Compatible.getAccessKeyId() != null && !s3Compatible.getAccessKeyId().isEmpty(),
        "s3_compatible.access_key_id is required.");
    ValidationUtils.checkArgument(
        s3Compatible.getSecretAccessKey() != null && !s3Compatible.getSecretAccessKey().isEmpty(),
        "s3_compatible.secret_access_key is required.");
    ValidationUtils.validateHttpUrl(s3Compatible.getS3Endpoint(), "s3_compatible.s3_endpoint");
    S3AssumeRole assumeRole = s3Compatible.getAssumeRole();
    if (assumeRole != null) {
      if (assumeRole.getStsEndpoint() != null) {
        ValidationUtils.validateHttpUrl(
            assumeRole.getStsEndpoint(), "s3_compatible.assume_role.sts_endpoint");
      }
      ValidationUtils.checkArgument(
          assumeRole.getRoleArn() != null && !assumeRole.getRoleArn().isEmpty(),
          "s3_compatible.assume_role.role_arn is required.");
      // A region name signs AssumeRole requests; storage may use its own names, such as "garage".
      ValidationUtils.checkArgument(
          assumeRole.getStsRegion() == null || assumeRole.getStsRegion().matches("[A-Za-z0-9._-]+"),
          "Invalid s3_compatible.assume_role.sts_region: use letters, digits, '.', '_' and '-'.");
    }
  }

  private <T> void setCredential(CredentialType type, T inputCredential) {
    credentialType = type;
    try {
      credential = objectMapper.writeValueAsString(inputCredential);
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

  private static S3CompatibleResponse fromS3CompatibleRequest(S3CompatibleRequest s3Compatible) {
    // An sts_endpoint not set stays absent, and AssumeRole goes to s3_endpoint at vend time, so a
    // read-modify-write that changes s3_endpoint moves STS too.
    return new S3CompatibleResponse()
        .accessKeyId(s3Compatible.getAccessKeyId())
        .s3Endpoint(s3Compatible.getS3Endpoint())
        .assumeRole(s3Compatible.getAssumeRole());
  }

  public AwsIamRoleResponse getAwsIamRoleResponse() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleResponse.class);
  }

  public S3CompatibleResponse getS3CompatibleResponse() {
    return parseCredential(CredentialType.S3_COMPATIBLE, S3CompatibleResponse.class);
  }

  /**
   * @param secretStore the store that keeps it
   * @return the write-only {@code secret_access_key} of an {@code s3_compatible} credential
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
