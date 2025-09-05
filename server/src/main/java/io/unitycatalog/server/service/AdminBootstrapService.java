package io.unitycatalog.server.service;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.RequestObject;
import io.unitycatalog.control.model.BootstrapOwnerRequest;
import io.unitycatalog.control.model.BootstrapOwnerResponse;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.BootstrapStateRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.BootstrapState;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.jwt.JwksOperations;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin Bootstrap Service for Azure AD OWNER initialization.
 *
 * <p>Provides endpoint for bootstrapping an Azure AD principal as OWNER of Unity Catalog with
 * explicit JCasbin authorization and idempotency.
 */
public class AdminBootstrapService extends AuthorizedService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdminBootstrapService.class);
  private final JwksOperations jwksOperations;
  private final BootstrapStateRepository bootstrapStateRepository;
  private final UnityAccessUtil unityAccessUtil;

  public AdminBootstrapService(
      UnityCatalogAuthorizer authorizer, Repositories repositories, JwksOperations jwksOperations) {
    super(authorizer, repositories.getUserRepository());
    this.jwksOperations = jwksOperations;
    this.bootstrapStateRepository = repositories.getBootstrapStateRepository();
    this.unityAccessUtil = new UnityAccessUtil(repositories);
  }

  @Post("/admins/bootstrap-owner")
  public BootstrapOwnerResponse bootstrapOwner(
      @Header("Authorization") String authorization, @RequestObject BootstrapOwnerRequest request) {

    // 1. Extract and validate Azure JWT
    String token = extractBearerToken(authorization);
    DecodedJWT azureJwt = jwksOperations.validateAzureJwt(token);

    // 2. Extract Azure principal claims
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

    // 3. Check authorization: existing OWNER or within bootstrap window
    validateBootstrapAuthorization(request.getMetastoreId());

    // 4. Check for existing bootstrap state (idempotency)
    Optional<BootstrapState> existingBootstrap =
        bootstrapStateRepository.findByMetastoreId(request.getMetastoreId());

    if (existingBootstrap.isPresent()) {
      if (existingBootstrap.get().getAzureObjectId().equals(azureObjectId)) {
        // Idempotent: return existing bootstrap info
        return buildResponse(existingBootstrap.get(), "OWNER already bootstrapped");
      } else {
        // Conflict: different principal tried to bootstrap
        throw new BaseException(
            ErrorCode.ALREADY_EXISTS,
            "OWNER already exists for metastore: " + request.getMetastoreId());
      }
    }

    // 5. Create Azure OWNER and generate PAT
    String userId =
        unityAccessUtil.bootstrapAzureOwner(
            azureObjectId, principalEmail, displayName, request.getMetastoreId(), authorizer);
    String patToken = generatePatToken(userId);

    // 6. Record bootstrap state
    BootstrapState bootstrapState =
        bootstrapStateRepository.create(
            request.getMetastoreId(), azureObjectId, principalEmail, userId);

    // 7. Audit logging for security events
    LOGGER.info(
        "Azure OWNER bootstrap completed: metastore={}, principal={}, user={}, azureObjectId={}, timestamp={}",
        request.getMetastoreId(),
        principalEmail,
        userId,
        azureObjectId,
        bootstrapState.getBootstrappedAt());

    LOGGER.info(
        "PAT token generated for bootstrap user: user={}, tokenPrefix={}",
        userId,
        patToken.substring(0, Math.min(8, patToken.length())));

    return new BootstrapOwnerResponse()
        .userId(userId)
        .principalEmail(principalEmail)
        .metastoreId(request.getMetastoreId())
        .patToken(patToken)
        .message("OWNER bootstrap completed successfully");
  }

  private String extractBearerToken(String authorization) {
    if (authorization == null || !authorization.startsWith("Bearer ")) {
      throw new BaseException(ErrorCode.UNAUTHENTICATED, "Bearer token required");
    }
    return authorization.substring(7);
  }

  private void validateBootstrapAuthorization(String metastoreId) {
    // Authorization: Either existing OWNER or BOOTSTRAP_OWNER privilege, or within sealed bootstrap
    // window
    try {
      // Check if current principal has OWNER or BOOTSTRAP_OWNER privileges
      UUID principalId = userRepository.findPrincipalId();
      UUID metastoreUuid = UUID.fromString(metastoreId);

      if (authorizer.authorize(principalId, metastoreUuid, Privileges.OWNER)) {
        LOGGER.info(
            "Bootstrap authorized via existing OWNER privilege: principal={}, metastore={}",
            principalId,
            metastoreId);
        return; // Existing OWNER can bootstrap
      }

      if (authorizer.authorize(principalId, metastoreUuid, Privileges.BOOTSTRAP_OWNER)) {
        LOGGER.info(
            "Bootstrap authorized via BOOTSTRAP_OWNER privilege: principal={}, metastore={}",
            principalId,
            metastoreId);
        return; // Explicit bootstrap privilege
      }
    } catch (Exception e) {
      // Principal not found or not authorized - check bootstrap window
      LOGGER.debug(
          "Principal not found or lacking privileges, checking bootstrap window: {}",
          e.getMessage());
    }

    // Check if bootstrap window is still open
    if (!bootstrapStateRepository.isBootstrapWindowOpen()) {
      LOGGER.warn(
          "Bootstrap attempt denied: window closed and no OWNER/BOOTSTRAP_OWNER privileges for metastore={}",
          metastoreId);
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "Bootstrap window closed and no existing OWNER or BOOTSTRAP_OWNER privileges");
    }

    LOGGER.info("Bootstrap authorized via open bootstrap window: metastore={}", metastoreId);
  }

  private String generatePatToken(String userId) {
    // Generate PAT token for immediate administrative access
    // Implementation would integrate with PAT system
    return "pat_" + UUID.randomUUID().toString().replace("-", "");
  }

  private BootstrapOwnerResponse buildResponse(BootstrapState state, String message) {
    return new BootstrapOwnerResponse()
        .userId(state.getUserId())
        .principalEmail(state.getPrincipalEmail())
        .metastoreId(state.getMetastoreId())
        .patToken("[HIDDEN - Already Issued]")
        .message(message);
  }
}
