package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO.CredentialType;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.aws.AwsCredentialGenerator;
import io.unitycatalog.server.service.credential.aws.S3BucketRegions;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import io.unitycatalog.server.utils.UriScheme;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Instant;
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
  // Read once at startup, so one server never mixes the two modes.
  private final S3CompatibleStaticKeyVending staticKeyVending;

  public StorageCredentialVendorImpl(
      CloudCredentialVendor cloudCredentialVendor,
      ExternalLocationUtils externalLocationUtils,
      ServerProperties serverProperties) {
    this(cloudCredentialVendor, externalLocationUtils, serverProperties, new S3BucketRegions());
  }

  /**
   * @param serverProperties says whether S3-compatible storage is enabled, and what clients receive
   *     of a key pair
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
    this.staticKeyVending = serverProperties.getS3CompatibleStaticKeyVending();
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
    // Requests to S3-compatible storage are signed with the region it reports, which is part of
    // the vend cache key, so a region reported later selects its own entry.
    Optional<String> s3Region =
        s3Endpoint.map(
            endpoint ->
                s3BucketRegions.getS3CompatibleRegion(endpoint, path.toUri().getAuthority()));
    if (isEndpointOnlyForClient(credentialDAO, use)) {
      // The key pair is not vended to clients, so its secret is not loaded: only the S3 endpoint
      // and region, and the client uses its own keys. The lifetime, as for a vended key pair, tells
      // clients when they may fetch again to pick up a changed endpoint.
      return new TemporaryCredentials()
          .awsTempCredentials(
              new AwsCredentials()
                  .s3Endpoint(s3Endpoint.orElseThrow())
                  .s3Region(s3Region.orElse(null)))
          .expirationTime(
              Instant.now().plus(AwsCredentialGenerator.SESSION_DURATION).toEpochMilli())
          .url(path.toString());
    }
    CredentialContext credentialContext =
        CredentialContext.create(path, privileges, credentialDAO, s3Endpoint, s3Region);
    return cloudCredentialVendor.vendCredential(credentialContext).url(path.toString());
  }

  /**
   * The client policy for {@code s3_compatible} credentials, applied here rather than in vending or
   * its cache, which serve every caller alike: without {@code assume_role}, a client gets the key
   * pair only when {@code server.s3-compatible-storage.static-key-vending} is {@code static-key}.
   * The server and its storage cleanup always get usable keys.
   *
   * @return whether {@code use} gets only the S3 endpoint and region of the path's credential
   */
  private boolean isEndpointOnlyForClient(
      Optional<CredentialDAO> credentialDAO, CredentialUse use) {
    // A switch, so that a new purpose has to decide whether it is a client.
    boolean isClient =
        switch (use.getPurpose()) {
          case CLIENT -> true;
          case SERVER, CLEANUP -> false;
        };
    return isClient
        && staticKeyVending == S3CompatibleStaticKeyVending.ENDPOINT_ONLY
        && credentialDAO
            .filter(dao -> dao.getCredentialType() == CredentialType.S3_COMPATIBLE)
            .filter(dao -> dao.getS3CompatibleResponse().getAssumeRole() == null)
            .isPresent();
  }
}
