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

public class UnityAccessUtil {

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
}
