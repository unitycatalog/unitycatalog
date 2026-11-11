package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.model.AzureServicePrincipalResponse;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.model.GcpServiceAccountKeyResponse;
import io.unitycatalog.server.service.CredentialService;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.utils.UriScheme;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
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
 * <p>The blob holds what the server needs to vend credentials, including the Azure client secret
 * and the GCP private key. {@link #toCredentialInfo} leaves those out, so they never reach an API
 * response.
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

  /** The cloud identity a credential holds, and the storage URL schemes that identity can reach. */
  public enum CredentialType {
    AWS_IAM_ROLE("aws_iam_role", Set.of(UriScheme.S3)),
    AZURE_SERVICE_PRINCIPAL("azure_service_principal", Set.of(UriScheme.ABFS, UriScheme.ABFSS)),
    GCP_SERVICE_ACCOUNT_KEY("gcp_service_account_key", Set.of(UriScheme.GS));

    /** The request field that carries this type, as named in the API. */
    private final String apiField;

    private final Set<UriScheme> schemes;

    CredentialType(String apiField, Set<UriScheme> schemes) {
      this.apiField = apiField;
      this.schemes = schemes;
    }

    public String getApiField() {
      return apiField;
    }

    /**
     * Whether this credential can vend access to a location with the given scheme. Local {@code
     * file:} locations need no cloud credential, so any type is accepted for them.
     */
    public boolean supports(UriScheme scheme) {
      return scheme == UriScheme.FILE || scheme == UriScheme.NULL || schemes.contains(scheme);
    }
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
   * Builds a DAO for a create request that carries exactly one cloud identity; the caller validates
   * that before calling.
   */
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
    } else if (createRequest.getAzureServicePrincipal() != null) {
      dao.setAzureServicePrincipal(createRequest.getAzureServicePrincipal());
    } else if (createRequest.getGcpServiceAccountKey() != null) {
      dao.setGcpServiceAccountKey(createRequest.getGcpServiceAccountKey());
    } else {
      throw new IllegalArgumentException("Unknown credential type");
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
      case AWS_IAM_ROLE -> {
        AwsIamRoleResponse awsIamRole = parseCredential(AwsIamRoleResponse.class);
        masterAwsIamRoleArn.ifPresent(awsIamRole::setUnityCatalogIamArn);
        credentialInfo.setAwsIamRole(awsIamRole);
      }
      case AZURE_SERVICE_PRINCIPAL -> {
        AzureServicePrincipalRequest servicePrincipal =
            parseCredential(AzureServicePrincipalRequest.class);
        credentialInfo.setAzureServicePrincipal(
            new AzureServicePrincipalResponse()
                .directoryId(servicePrincipal.getDirectoryId())
                .applicationId(servicePrincipal.getApplicationId()));
      }
      case GCP_SERVICE_ACCOUNT_KEY -> {
        GcpServiceAccountKeyRequest key = parseCredential(GcpServiceAccountKeyRequest.class);
        credentialInfo.setGcpServiceAccountKey(
            new GcpServiceAccountKeyResponse()
                .email(key.getEmail())
                .privateKeyId(key.getPrivateKeyId()));
      }
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
      throw new IllegalArgumentException(
          "Failed to parse credential of " + clazz.getSimpleName(), e);
    }
  }

  public void setAwsIamRole(AwsIamRoleRequest awsIamRole) {
    setCredential(CredentialType.AWS_IAM_ROLE, fromAwsIamRoleRequest(awsIamRole));
  }

  public void setAzureServicePrincipal(AzureServicePrincipalRequest servicePrincipal) {
    setCredential(CredentialType.AZURE_SERVICE_PRINCIPAL, servicePrincipal);
  }

  public void setGcpServiceAccountKey(GcpServiceAccountKeyRequest key) {
    setCredential(CredentialType.GCP_SERVICE_ACCOUNT_KEY, key);
  }

  private <T> void setCredential(CredentialType type, T inputCredential) {
    credentialType = type;
    try {
      credential = objectMapper.writeValueAsString(inputCredential);
      // TODO: encrypt the credential
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

  public AwsIamRoleResponse getAwsIamRoleResponse() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleResponse.class);
  }
}
