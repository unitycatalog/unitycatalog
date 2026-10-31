package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.UUID;

public class UnityAccessUtil {

  private final UserRepository userRepository;
  private final MetastoreRepository metastoreRepository;

  public UnityAccessUtil(Repositories repositories) {
    this.userRepository = repositories.getUserRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  /**
   * Returns the parent type in the stored authorization hierarchy, or null at a root.
   *
   * <p>Metastore ownership is checked separately; catalogs, external locations, and credentials
   * have no stored parent.
   */
  public static SecurableType getHierarchyParentType(SecurableType type) {
    return switch (type) {
      case TABLE, VOLUME, FUNCTION, REGISTERED_MODEL -> SecurableType.SCHEMA;
      case SCHEMA -> SecurableType.CATALOG;
      case METASTORE, CATALOG, EXTERNAL_LOCATION, CREDENTIAL -> null;
    };
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
      // Any other lookup error, such as an unreachable database, fails startup.
      if (e.getErrorCode() != ErrorCode.NOT_FOUND) {
        throw e;
      }
    }

    CreateUser createUser = CreateUser.builder().email("admin").name("Admin").build();

    User adminUser = userRepository.createUser(createUser);

    authorizer.grantAuthorization(
        UUID.fromString(adminUser.getId()), metastoreRepository.getMetastoreId(), Privileges.OWNER);
  }
}
