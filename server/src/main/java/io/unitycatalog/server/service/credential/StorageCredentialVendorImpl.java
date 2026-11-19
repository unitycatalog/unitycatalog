package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.S3AddressingStyle;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO.CredentialType;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils.LocationCredential;
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
    Optional<LocationCredential> locationCredential =
        externalLocationUtils.getExternalLocationCredentialForPath(path);
    Optional<CredentialDAO> credentialDAO = locationCredential.map(LocationCredential::credential);
    // A path on S3-compatible storage reaches the storage at its location's endpoint, addressed as
    // the location says.
    Optional<ExternalLocationDAO> s3CompatibleLocation =
        locationCredential
            .filter(found -> found.credential().getCredentialType() == CredentialType.S3_COMPATIBLE)
            .map(LocationCredential::location);
    Optional<String> s3Endpoint = s3CompatibleLocation.map(StorageCredentialVendorImpl::s3Endpoint);
    Optional<S3AddressingStyle> s3AddressingStyle =
        s3CompatibleLocation.map(ExternalLocationDAO::getS3AddressingStyle);
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
                    s3BucketRegions.getS3CompatibleRegion(
                        endpoint, path.toUri().getAuthority(), s3AddressingStyle.orElseThrow())),
            s3AddressingStyle,
            /* isServerAccess= */ isServerAccess(use.getPurpose()));
    return cloudCredentialVendor.vendCredential(credentialContext).url(path.toString());
  }

  /**
   * @param location an external location whose credential is {@code s3_compatible_access_key}
   * @return its endpoint, which creating and updating it require
   * @throws BaseException with INTERNAL if it has none, rather than vend credentials that a client
   *     would send to AWS S3
   */
  private static String s3Endpoint(ExternalLocationDAO location) {
    if (location.getEndpoint() == null) {
      throw new BaseException(
          ErrorCode.INTERNAL,
          String.format(
              "External location '%s' has an s3_compatible_access_key credential but no endpoint.",
              location.getName()));
    }
    return location.getEndpoint();
  }

  /** Whether the server itself uses the credentials, rather than returning them to the caller. */
  private static boolean isServerAccess(Purpose purpose) {
    return switch (purpose) {
      case SERVER, CLEANUP -> true;
      case CLIENT -> false;
    };
  }
}
