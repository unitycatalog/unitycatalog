package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityAccessUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessUtil.class);
  private final UserRepository userRepository;
  private final MetastoreRepository metastoreRepository;

  public UnityAccessUtil(Repositories repositories) {
    this.userRepository = repositories.getUserRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  public void initializeAdmin(UnityCatalogAuthorizer authorizer) {

    // If no admin user exists, lets create one and grant the admin as
    // the OWNER for this server. This is meant to allow a bootstrap
    // for the server and provide an admin account with which regular users
    // can be subsequently added.
    //
    // Note that if the user exists, we don't want to do anything else because
    // also as bootstrapping, one may want to deactivate or decrease the privileges
    // the admin user has.

    try {
      userRepository.getUserByEmail("admin");
      return;
    } catch (BaseException e) {
      // IGNORE - this should be user not found exception.
    }

    CreateUser createUser = CreateUser.builder().email("admin").name("Admin").build();

    User adminUser = userRepository.createUser(createUser);

    authorizer.grantAuthorization(
        UUID.fromString(adminUser.getId()), metastoreRepository.getMetastoreId(), Privileges.OWNER);
  }

  /**
   * Bootstrap an Azure AD principal as OWNER with explicit JCasbin binding. Called from
   * AdminBootstrapService with validated Azure JWT claims.
   *
   * @param azureObjectId Azure AD object ID from JWT 'oid' claim
   * @param principalEmail Email from JWT 'preferred_username' or 'upn' claim
   * @param displayName Display name from JWT 'name' claim
   * @param metastoreId Target metastore for OWNER privileges
   * @return Created user ID
   */
  public String bootstrapAzureOwner(
      String azureObjectId,
      String principalEmail,
      String displayName,
      String metastoreId,
      UnityCatalogAuthorizer authorizer) {

    // Check if user already exists by Azure object ID
    try {
      User existingUser = userRepository.getUserByExternalId(azureObjectId);
      if (existingUser != null) {
        LOGGER.info(
            "Azure principal already exists: objectId={}, userId={}",
            azureObjectId,
            existingUser.getId());
        return existingUser.getId();
      }
    } catch (BaseException e) {
      // User not found - proceed with creation
    }

    // Create new user with Azure external ID
    CreateUser createUser =
        CreateUser.builder()
            .email(principalEmail)
            .name(displayName)
            .externalId(azureObjectId) // Store Azure object ID
            .build();

    User azureUser = userRepository.createUser(createUser);

    // Explicit OWNER grant via JCasbin (line reference: JCasbinAuthorizer.java:94-98)
    UUID userUuid = UUID.fromString(azureUser.getId());
    UUID metastoreUuid = UUID.fromString(metastoreId);

    boolean granted = authorizer.grantAuthorization(userUuid, metastoreUuid, Privileges.OWNER);
    if (!granted) {
      throw new BaseException(
          io.unitycatalog.server.exception.ErrorCode.INTERNAL,
          "Failed to grant OWNER privileges to Azure principal");
    }

    LOGGER.info(
        "Azure OWNER bootstrap completed: objectId={}, userId={}, metastore={}",
        azureObjectId,
        azureUser.getId(),
        metastoreId);

    return azureUser.getId();
  }
}
