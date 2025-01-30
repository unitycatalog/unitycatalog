package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.utils.Scim2Utils.asUserResource;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Produces;
import com.linecorp.armeria.server.annotation.StatusCode;
import com.unboundid.scim2.common.exceptions.BadRequestException;
import com.unboundid.scim2.common.types.UserResource;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.exception.Scim2RuntimeException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.security.JwtClaim;

@ExceptionHandler(GlobalExceptionHandler.class)
public class Scim2SelfService {
  private final UserRepository userRepository;
  private final UnityCatalogAuthorizer authorizer;

  public Scim2SelfService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.userRepository = repositories.getUserRepository();
  }

  @Get("")
  @Produces("application/scim+json")
  @StatusCode(200)
  @AuthorizeExpression("#principal != null")
  @AuthorizeKey(METASTORE)
  public UserResource getCurrentUser() {
    // TODO: will make this a util method in the access control PR
    ServiceRequestContext ctx = ServiceRequestContext.current();
    DecodedJWT decodedJWT = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    if (decodedJWT != null) {
      Claim sub = decodedJWT.getClaim(JwtClaim.SUBJECT.key());
      return asUserResource(userRepository.getUserByEmail(sub.asString()));
    } else {
      throw new Scim2RuntimeException(new BadRequestException("No user found."));
    }
  }
}
