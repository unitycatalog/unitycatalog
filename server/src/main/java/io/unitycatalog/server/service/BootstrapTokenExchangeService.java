package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.control.model.AccessTokenType;
import io.unitycatalog.control.model.OAuthTokenExchangeInfo;
import io.unitycatalog.control.model.TokenType;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.CreateUser;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.security.jwt.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap Token Exchange Service for Azure AD user creation with token issuance.
 *
 * <p>Provides endpoint for obtaining Unity Catalog access tokens during the bootstrap window,
 * creating Azure AD users if they don't exist. This bridges the gap between Azure JWT validation
 * and Unity Catalog token issuance for bootstrap scenarios.
 */
public class BootstrapTokenExchangeService extends AuthorizedService {

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapTokenExchangeService.class);
  private final UnityCatalogAuthorizer authorizer;
  private final JwksOperations jwksOperations;
  private final UnityAccessUtil unityAccessUtil;
  private final ServerProperties serverProperties;
  private final SecurityContext securityContext;
  private final UserRepository userRepository;
  private final MetastoreRepository metastoreRepository;

  public BootstrapTokenExchangeService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      JwksOperations jwksOperations,
      ServerProperties serverProperties,
      SecurityContext securityContext) {
    super(authorizer, repositories.getUserRepository());
    this.authorizer = authorizer;
    this.jwksOperations = jwksOperations;
    this.unityAccessUtil = new UnityAccessUtil(repositories);
    this.serverProperties = serverProperties;
    this.securityContext = securityContext;
    this.userRepository = repositories.getUserRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("/token-exchange")
  @ProducesJson
  public OAuthTokenExchangeInfo bootstrapTokenExchange(
      @Header("Authorization") String authorization) {

    // 1. Validate bootstrap is enabled
    if (!Boolean.parseBoolean(serverProperties.getProperty("bootstrap.enabled", "false"))) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Bootstrap is disabled");
    }

    // 2. Extract and validate Azure JWT
    String token = extractBearerToken(authorization);
    DecodedJWT azureJwt = jwksOperations.validateAzureJwt(token);

    // 3. Extract principal claims
    String azureObjectId = azureJwt.getClaim("oid").asString();
    String principalEmail =
        Optional.ofNullable(azureJwt.getClaim("email").asString())
            .orElse(
                Optional.ofNullable(azureJwt.getClaim("preferred_username").asString())
                    .orElse(azureJwt.getClaim("upn").asString()));
    String displayName = azureJwt.getClaim("name").asString();

    if (azureObjectId == null || principalEmail == null || displayName == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Missing required Azure JWT claims: oid, email/preferred_username/upn, name");
    }

    // 4. Check domain restrictions (if configured)
    validateAllowedDomain(principalEmail);

    // 5. Create or get existing user and grant OWNER privileges during bootstrap
    String userId = createOrUpdateAzureUser(azureObjectId, principalEmail, displayName);

    // 6. Grant OWNER privileges for bootstrap (equivalent to local admin)
    grantBootstrapOwnerPrivileges(userId);

    // 7. Create JWT with proper claims for token generation
    DecodedJWT enhancedJwt = createEnhancedJwt(azureJwt, principalEmail);

    // 8. Generate Unity Catalog access token
    String accessToken = securityContext.createAccessToken(enhancedJwt);

    LOGGER.info(
        "Bootstrap token exchange completed: principal={}, user={}, azureObjectId={}",
        principalEmail,
        userId,
        azureObjectId);

    return new OAuthTokenExchangeInfo()
        .accessToken(accessToken)
        .issuedTokenType(TokenType.ACCESS_TOKEN)
        .tokenType(AccessTokenType.BEARER);
  }

  private String extractBearerToken(String authorization) {
    if (authorization == null || !authorization.startsWith("Bearer ")) {
      throw new BaseException(
          ErrorCode.UNAUTHENTICATED, "Authorization header must contain Bearer token");
    }
    return authorization.substring(7);
  }

  private void validateAllowedDomain(String email) {
    String allowedDomainsProperty = serverProperties.getProperty("bootstrap.allowedDomains", "");
    if (allowedDomainsProperty.isEmpty()) {
      return; // No domain restrictions
    }

    List<String> allowedDomains = Arrays.asList(allowedDomainsProperty.split(","));
    String emailDomain = email.substring(email.indexOf("@") + 1);

    if (!allowedDomains.contains(emailDomain)) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED, "Email domain not allowed for bootstrap: " + emailDomain);
    }
  }

  private String createOrUpdateAzureUser(String azureObjectId, String email, String displayName) {
    try {
      // Check if user already exists by email
      User existingUser = userRepository.getUserByEmail(email);
      if (existingUser != null) {
        LOGGER.info(
            "Azure principal already exists in database: email={}, userId={}",
            email,
            existingUser.getId());
        return existingUser.getId();
      }
    } catch (BaseException e) {
      // User not found by email - check by external ID
      try {
        User existingUser = userRepository.getUserByExternalId(azureObjectId);
        if (existingUser != null) {
          LOGGER.info(
              "Azure principal found by object ID: objectId={}, userId={}",
              azureObjectId,
              existingUser.getId());
          return existingUser.getId();
        }
      } catch (BaseException e2) {
        // User not found by external ID either - will create new user
      }
    }

    // Create new user with Azure external ID
    CreateUser createUser =
        CreateUser.builder()
            .email(email)
            .name(displayName)
            .externalId(azureObjectId) // Store Azure object ID
            .build();

    User azureUser = userRepository.createUser(createUser);

    LOGGER.info(
        "Created new Azure user during bootstrap: objectId={}, userId={}, email={}",
        azureObjectId,
        azureUser.getId(),
        email);

    return azureUser.getId();
  }

  private void grantBootstrapOwnerPrivileges(String userId) {
    // Grant OWNER privileges equivalent to what the local admin gets
    // This is the same logic as UnityAccessUtil.initializeAdmin()
    UUID userUuid = UUID.fromString(userId);
    UUID metastoreUuid = metastoreRepository.getMetastoreId();

    LOGGER.info(
        "BOOTSTRAP: About to grant OWNER privileges: userId={}, metastore={}, privilege={}",
        userId,
        metastoreUuid,
        io.unitycatalog.server.persist.model.Privileges.OWNER);

    // Check if user already has OWNER privileges
    boolean hasOwnerPrivileges =
        authorizer.authorize(
            userUuid, metastoreUuid, io.unitycatalog.server.persist.model.Privileges.OWNER);

    if (hasOwnerPrivileges) {
      LOGGER.info(
          "BOOTSTRAP: User already has OWNER privileges: userId={}, metastore={}",
          userId,
          metastoreUuid);
      return;
    }

    boolean granted =
        authorizer.grantAuthorization(
            userUuid, metastoreUuid, io.unitycatalog.server.persist.model.Privileges.OWNER);

    LOGGER.info(
        "BOOTSTRAP: Authorization grant result: granted={}, userId={}, metastore={}",
        granted,
        userId,
        metastoreUuid);

    if (!granted) {
      throw new BaseException(
          ErrorCode.INTERNAL, "Failed to grant OWNER privileges during bootstrap");
    }

    LOGGER.info(
        "BOOTSTRAP: Successfully granted OWNER privileges during bootstrap: userId={}, metastore={}",
        userId,
        metastoreUuid);
  }

  private DecodedJWT createEnhancedJwt(DecodedJWT originalJwt, String principalEmail) {
    // For now, return the original JWT but ensure it has the email claim
    // The SecurityContext.createAccessToken should work with the Azure JWT as-is
    // since it extracts subject from either email or subject claims
    return originalJwt;
  }
}
