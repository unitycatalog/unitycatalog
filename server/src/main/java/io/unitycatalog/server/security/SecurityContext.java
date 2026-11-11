package io.unitycatalog.server.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityContext {

  public interface Issuers {
    String INTERNAL = "internal";
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SecurityContext.class);

  private final Path serviceTokenFile;

  @Getter private final Algorithm algorithm;
  @Getter private final String serviceName;
  @Getter private final String localIssuer;

  public SecurityContext(
      Path configurationFolder,
      SecurityConfiguration securityConfiguration,
      String serviceName,
      String localIssuer) {
    this.serviceName = serviceName;
    this.localIssuer = localIssuer;

    serviceTokenFile = configurationFolder.resolve("token.txt");

    algorithm = securityConfiguration.algorithmRSA();

    deleteServiceTokenFile();
  }

  public String createAccessToken(String principalEmail, Duration ttl) {
    Instant expiresAt = Instant.now().plus(ttl);
    return JWT.create()
        .withSubject(serviceName)
        .withIssuer(localIssuer)
        .withIssuedAt(new Date())
        .withExpiresAt(Date.from(expiresAt))
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
        .withClaim(JwtClaim.SUBJECT.key(), principalEmail)
        .sign(algorithm);
  }

  public String createServiceToken() {
    return createServiceToken(algorithm, serviceName, localIssuer);
  }

  /** Creates an admin service token. It has no expiry, so it is valid while the key is. */
  static String createServiceToken(Algorithm algorithm, String serviceName, String localIssuer) {
    return JWT.create()
        .withSubject(serviceName)
        .withIssuer(localIssuer)
        .withIssuedAt(new Date())
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.SERVICE.name())
        .withClaim(JwtClaim.SUBJECT.key(), "admin")
        .sign(algorithm);
  }

  /**
   * Creates an IdP's SCIM token: it names the IdP principal by its user id, and expires after
   * {@code lifetime}.
   */
  static String createScimToken(
      Algorithm algorithm,
      String serviceName,
      String localIssuer,
      String principalId,
      String idpName,
      Duration lifetime) {
    return JWT.create()
        .withSubject(serviceName)
        .withIssuer(localIssuer)
        .withIssuedAt(new Date())
        .withExpiresAt(Date.from(Instant.now().plus(lifetime)))
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.SCIM.name())
        .withClaim(JwtClaim.SUBJECT.key(), principalId)
        .withClaim(JwtClaim.IDP.key(), idpName)
        .sign(algorithm);
  }

  // Earlier versions wrote an admin token that does not expire to token.txt in the configuration
  // folder, so remove that plaintext copy; bin/uc-admin-token prints an admin token when needed.
  // Removing the file does not revoke copies; changing the signing key does.
  @SneakyThrows
  private void deleteServiceTokenFile() {
    if (Files.deleteIfExists(serviceTokenFile)) {
      LOGGER.warn(
          "Deleted {}: the server no longer writes an admin token, and bin/uc-admin-token"
              + " prints one. Copies of the deleted token stay valid until the signing key"
              + " changes.",
          serviceTokenFile);
    }
  }
}
