package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.utils.NormalizedURL;
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
public class StorageCredentialVendor {

  private final CloudCredentialVendor cloudCredentialVendor;
  private final ExternalLocationUtils externalLocationUtils;

  public StorageCredentialVendor(
      CloudCredentialVendor cloudCredentialVendor, ExternalLocationUtils externalLocationUtils) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.externalLocationUtils = externalLocationUtils;
  }

  /**
   * Vends temporary credentials for accessing the specified storage path.
   *
   * @param path the normalized storage location URL (e.g., s3://bucket/path)
   * @param privileges the set of privileges required (SELECT, UPDATE)
   * @return temporary credentials scoped to the requested path and privileges
   * @throws BaseException if the path is null or credentials cannot be generated
   */
  public TemporaryCredentials vendCredential(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    if (path == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Storage location is null.");
    }
    if (privileges == null || privileges.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Privileges cannot be null or empty.");
    }

    // Permission authorization is already done before calling this function.
    Optional<CredentialDAO> credentialDAO =
        externalLocationUtils.getExternalLocationCredentialForPath(path);
    CredentialContext credentialContext = CredentialContext.create(path, privileges, credentialDAO);
    return cloudCredentialVendor.vendCredential(credentialContext);
  }
}
