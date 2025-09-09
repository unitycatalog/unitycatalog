package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.RequestObject;
import io.unitycatalog.control.model.CreateDeveloperTokenRequest;
import io.unitycatalog.control.model.CreateDeveloperTokenResponse;
import io.unitycatalog.control.model.DeveloperTokenInfo;
import io.unitycatalog.control.model.ListDeveloperTokensResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.persist.DeveloperTokenRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.DeveloperTokenDAO;
import io.unitycatalog.server.utils.IdentityUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class DeveloperTokenService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeveloperTokenService.class);
  private static final String TOKEN_PREFIX = "dapi_";
  private static final SecureRandom secureRandom = new SecureRandom();

  private final DeveloperTokenRepository developerTokenRepository;

  @SneakyThrows
  public DeveloperTokenService(Repositories repositories) {
    this.developerTokenRepository = repositories.getDeveloperTokenRepository();
  }

  @Post("")
  public HttpResponse createDeveloperToken(
      ServiceRequestContext ctx, @RequestObject CreateDeveloperTokenRequest request) {

    String userEmail = IdentityUtils.findPrincipalEmailAddress();
    if (userEmail == null) {
      throw new BaseException(ErrorCode.UNAUTHENTICATED, "User not authenticated");
    }

    // Validate lifetime (max 60 days in seconds)
    Integer lifetimeSeconds = request.getLifetimeSeconds();
    if (lifetimeSeconds != null && lifetimeSeconds > 60 * 24 * 60 * 60) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Token lifetime cannot exceed 60 days");
    }

    // Generate secure token
    byte[] randomBytes = new byte[32];
    secureRandom.nextBytes(randomBytes);
    String tokenValue =
        TOKEN_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);

    // Hash the token for storage
    String tokenHash = hashToken(tokenValue);

    // Calculate expiry time
    long lifetimeMs = lifetimeSeconds != null ? lifetimeSeconds * 1000L : 30L * 24 * 60 * 60 * 1000;
    long expiryTime = System.currentTimeMillis() + lifetimeMs;

    // Create DAO object
    DeveloperTokenDAO tokenDAO =
        DeveloperTokenDAO.builder()
            .id(UUID.randomUUID())
            .userId(userEmail)
            .tokenHash(tokenHash)
            .comment(request.getComment())
            .creationTime(new Date())
            .expiryTime(new Date(expiryTime))
            .status(DeveloperTokenDAO.TokenStatus.ACTIVE)
            .build();

    // Save to database
    DeveloperTokenDAO savedToken = developerTokenRepository.createToken(tokenDAO);

    // Return response with token value (shown only once)
    CreateDeveloperTokenResponse response = new CreateDeveloperTokenResponse();
    response.setTokenId(savedToken.getId().toString());
    response.setTokenValue(tokenValue);
    response.setComment(savedToken.getComment());
    response.setExpiryTime(savedToken.getExpiryTime().getTime());

    return HttpResponse.ofJson(response);
  }

  @Get("")
  public HttpResponse listDeveloperTokens(ServiceRequestContext ctx) {

    String userEmail = IdentityUtils.findPrincipalEmailAddress();
    if (userEmail == null) {
      throw new BaseException(ErrorCode.UNAUTHENTICATED, "User not authenticated");
    }

    List<DeveloperTokenDAO> tokens = developerTokenRepository.listTokensByUser(userEmail);

    List<DeveloperTokenInfo> developerTokens =
        tokens.stream().map(this::convertToModel).collect(Collectors.toList());

    ListDeveloperTokensResponse response = new ListDeveloperTokensResponse();
    response.setTokens(developerTokens);

    return HttpResponse.ofJson(response);
  }

  @Delete("/{tokenId}")
  public HttpResponse revokeDeveloperToken(
      ServiceRequestContext ctx, @Param("tokenId") String tokenId) {

    String userEmail = IdentityUtils.findPrincipalEmailAddress();
    if (userEmail == null) {
      throw new BaseException(ErrorCode.UNAUTHENTICATED, "User not authenticated");
    }

    developerTokenRepository.revokeToken(tokenId, userEmail);
    return HttpResponse.of(200);
  }

  private String hashToken(String token) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(token.getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(hash);
    } catch (Exception e) {
      throw new BaseException(ErrorCode.INTERNAL, "Failed to hash token");
    }
  }

  private DeveloperTokenInfo convertToModel(DeveloperTokenDAO dao) {
    DeveloperTokenInfo info = new DeveloperTokenInfo();
    info.setTokenId(dao.getId().toString());
    info.setComment(dao.getComment());
    info.setCreationTime(dao.getCreationTime().getTime());
    info.setExpiryTime(dao.getExpiryTime().getTime());
    info.setTokenStatus(DeveloperTokenInfo.TokenStatusEnum.fromValue(dao.getStatus().name()));
    return info;
  }

  public Optional<String> validateToken(String token) {
    if (!token.startsWith(TOKEN_PREFIX)) {
      return Optional.empty();
    }

    String tokenHash = hashToken(token);
    Optional<DeveloperTokenDAO> tokenDAO = developerTokenRepository.getTokenByHash(tokenHash);

    if (tokenDAO.isPresent()) {
      DeveloperTokenDAO dao = tokenDAO.get();
      // Check if token is active and not expired
      if (dao.getStatus() == DeveloperTokenDAO.TokenStatus.ACTIVE
          && dao.getExpiryTime().getTime() > System.currentTimeMillis()) {
        return Optional.of(dao.getUserId());
      }
    }

    return Optional.empty();
  }
}
