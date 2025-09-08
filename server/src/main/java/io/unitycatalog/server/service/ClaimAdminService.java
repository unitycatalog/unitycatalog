package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.control.model.ClaimAdminResponse;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.security.jwt.JwksOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Claim Admin Service for Azure AD admin bootstrap. Allows first Azure principal to claim OWNER
 * when no Azure admin exists.
 */
public class ClaimAdminService extends AuthorizedService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClaimAdminService.class);
  private final JwksOperations jwksOperations;
  private final UnityAccessUtil unityAccessUtil;
  private final ServerProperties serverProperties;

  public ClaimAdminService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      JwksOperations jwksOperations,
      ServerProperties serverProperties) {
    super(authorizer, repositories.getUserRepository());
    this.jwksOperations = jwksOperations;
    this.unityAccessUtil = new UnityAccessUtil(repositories);
    this.serverProperties = serverProperties;
  }

  @Post("/claim-admin")
  @ProducesJson
  public ClaimAdminResponse claimAdmin(@Header("Authorization") String authorization) {

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
        Optional.ofNullable(azureJwt.getClaim("preferred_username").asString())
            .orElse(azureJwt.getClaim("upn").asString());
    String displayName = azureJwt.getClaim("name").asString();

    if (azureObjectId == null || principalEmail == null || displayName == null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Missing required Azure JWT claims: oid, preferred_username/upn, name");
    }

    // 4. Check domain restrictions
    validateAllowedDomain(principalEmail);

    // 5. Check if Azure admin already exists
    if (hasAzureAuthenticatedOwner()) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "An Azure-authenticated admin already exists");
    }

    // 6. Create/update user and bind OWNER role
    String userId = createOrUpdateAzureUser(azureObjectId, principalEmail, displayName);
    bindOwnerRole(userId);

    LOGGER.info(
        "Azure admin claimed: principal={}, user={}, azureObjectId={}",
        principalEmail,
        userId,
        azureObjectId);

    return new ClaimAdminResponse()
        .userId(userId)
        .principalEmail(principalEmail)
        .message(
            "Admin privileges claimed successfully. Use 'bin/uc auth login --output jsonPretty' to continue.");
  }

  private void validateAllowedDomain(String email) {
    String allowedDomainsStr = serverProperties.getProperty("bootstrap.allowedDomains", "");
    if (allowedDomainsStr.isEmpty()) return; // No restrictions

    List<String> allowedDomains = Arrays.asList(allowedDomainsStr.split(","));
    String domain = email.substring(email.indexOf('@') + 1);

    if (!allowedDomains.contains(domain)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Email domain not allowed: " + domain);
    }
  }

  private boolean hasAzureAuthenticatedOwner() {
    // TODO: Implement actual check for existing Azure admin
    return false;
  }

  private String createOrUpdateAzureUser(String azureObjectId, String email, String displayName) {
    // TODO: Implement user creation/update logic
    return UUID.randomUUID().toString();
  }

  private void bindOwnerRole(String userId) {
    // TODO: Implement JCasbin OWNER role binding
    LOGGER.info("Binding OWNER role to user: {}", userId);
  }

  private String extractBearerToken(String authorization) {
    if (authorization == null || !authorization.startsWith("Bearer ")) {
      throw new BaseException(ErrorCode.UNAUTHENTICATED, "Bearer token required");
    }
    return authorization.substring(7);
  }
}
