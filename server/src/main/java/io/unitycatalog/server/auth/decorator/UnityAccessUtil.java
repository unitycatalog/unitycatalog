package io.unitycatalog.server.auth.decorator;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import java.util.UUID;

public class UnityAccessUtil {

  private static final UserRepository USER_REPOSITORY = UserRepository.getInstance();
  private static final MetastoreRepository METASTORE_REPOSITORY = MetastoreRepository.getInstance();

  public static UUID findPrincipalId() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    // TODO: if/when authorization becomes mandatory, maybe just throw an exception here?
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return UUID.fromString(USER_REPOSITORY.getUserByEmail(sub.asString()).getId());
    } else {
      return null;
    }
  }

  public static void initializeAdmin(UnityCatalogAuthorizer authorizer) {

    // If no admin user exists, lets create one and grant the admin as
    // the OWNER for this server. This is meant to allow a bootstrap
    // for the server and provide an admin account with which regular users
    // can be subsequently added.
    //
    // Note that if the user exists, we don't want to do anything else because
    // also as bootstrapping, one may want to deactivate or decrease the privileges
    // the admin user has.

    try {
      USER_REPOSITORY.getUserByEmail("admin");
      return;
    } catch (BaseException e) {
      // IGNORE - this should be user not found exception.
    }

    CreateUser createUser = CreateUser.builder().email("admin").name("Admin").build();

    User adminUser = USER_REPOSITORY.createUser(createUser);

    authorizer.grantAuthorization(
        UUID.fromString(adminUser.getId()),
        METASTORE_REPOSITORY.getMetastoreId(),
        Privileges.OWNER);
  }
}
