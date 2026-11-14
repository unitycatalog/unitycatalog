package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO.CredentialType;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.aws.S3BucketRegions;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Optional;
import java.util.Set;

/**
 * Vends temporary cloud storage credentials for accessing data at specific paths.
 *
 * <p>This class is responsible for:
 *
 * <ul>
 *   <li>Looking up external location credentials based on the requested path
 *   <li>Delegating to cloud-specific credential vendors (AWS, Azure, GCP)
 *   <li>Returning scoped temporary credentials with appropriate privileges
 * </ul>
 */
public class StorageCredentialVendorImpl implements StorageCredentialVendor {

  private final CloudCredentialVendor cloudCredentialVendor;
  private final ExternalLocationUtils externalLocationUtils;
  private final ServerProperties serverProperties;
  private final S3BucketRegions s3BucketRegions;

  public StorageCredentialVendorImpl(
      CloudCredentialVendor cloudCredentialVendor,
      ExternalLocationUtils externalLocationUtils,
      ServerProperties serverProperties) {
    this(cloudCredentialVendor, externalLocationUtils, serverProperties, new S3BucketRegions());
  }

  /**
   * @param serverProperties says whether S3-compatible storage is enabled
   * @param s3BucketRegions discovers the region of buckets on S3-compatible storage
   */
  public StorageCredentialVendorImpl(
      CloudCredentialVendor cloudCredentialVendor,
      ExternalLocationUtils externalLocationUtils,
      ServerProperties serverProperties,
      S3BucketRegions s3BucketRegions) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.externalLocationUtils = externalLocationUtils;
    this.serverProperties = serverProperties;
    this.s3BucketRegions = s3BucketRegions;
  }

  /**
   * {@inheritDoc}
   *
   * @throws BaseException if the path is null or a cloud storage root, or credentials cannot be
   *     generated
   */
  @Override
  public TemporaryCredentials vendCredential(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges, CredentialUse use) {
    if (path == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Storage location is null.");
    }
    ValidationUtils.checkArgument(
        !path.isCloudStorageRoot(),
        "Storage location must include a non-empty path prefix before credentials can be vended:"
            + " %s",
        path);
    if (privileges == null || privileges.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Privileges cannot be null or empty.");
    }
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      // A local path has no credential to vend, so skip the external-location lookup.
      return new TemporaryCredentials().url(path.toString());
    }

    // Permission authorization is already done before calling this function.
    Optional<CredentialDAO> credentialDAO =
        externalLocationUtils.getExternalLocationCredentialDaoForPath(path);
    // A path on S3-compatible storage reaches the storage of its location's credential.
    Optional<String> s3Endpoint =
        credentialDAO
            .filter(dao -> dao.getCredentialType() == CredentialType.S3_COMPATIBLE)
            .map(dao -> dao.getS3CompatibleResponse().getS3Endpoint());
    if (s3Endpoint.isPresent()) {
      // Before the region lookup, so a disabled feature never contacts the stored endpoint.
      serverProperties.checkS3CompatibleStorageEnabled();
    }
    CredentialContext credentialContext =
        CredentialContext.create(
            path,
            privileges,
            credentialDAO,
            s3Endpoint,
            // Requests to S3-compatible storage are signed with the region it reports, which is
            // part of the vend cache key, so a region reported later selects its own entry.
            s3Endpoint.map(
                endpoint ->
                    s3BucketRegions.getS3CompatibleRegion(endpoint, path.toUri().getAuthority())),
            /* isServerAccess= */ isServerAccess(use.getPurpose()));
    return cloudCredentialVendor.vendCredential(credentialContext).url(path.toString());
  }

  /** Whether the server itself uses the credentials, rather than returning them to the caller. */
  private static boolean isServerAccess(Purpose purpose) {
    return switch (purpose) {
      case SERVER, CLEANUP -> true;
      case CLIENT -> false;
    };
  }
}
