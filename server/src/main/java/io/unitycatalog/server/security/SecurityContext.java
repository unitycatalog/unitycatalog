package io.unitycatalog.server.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import java.io.IOException;
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
  @Getter private final String serviceToken;
  @Getter private final String serviceName;
  @Getter private final String localIssuer;

  @SneakyThrows
  public SecurityContext(
      Path configurationFolder,
      SecurityConfiguration securityConfiguration,
      String serviceName,
      String localIssuer) {
    this.serviceName = serviceName;
    this.localIssuer = localIssuer;

    serviceTokenFile = configurationFolder.resolve("token.txt");

    algorithm = securityConfiguration.algorithmRSA();

    serviceToken = createServiceToken();

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

  // The admin token stays in memory; bin/uc-admin-token prints one. A token.txt in the
  // configuration folder is a plaintext copy of an admin token that does not expire, so remove it.
  // Removing the file does not revoke copies; changing the signing key does.
  private void deleteServiceTokenFile() {
    try {
      if (Files.deleteIfExists(serviceTokenFile)) {
        LOGGER.warn(
            "Deleted {}: the server keeps the admin token in memory, and bin/uc-admin-token prints"
                + " one. Copies of the deleted token stay valid until the signing key changes.",
            serviceTokenFile);
      }
    } catch (IOException e) {
      // A read-only configuration folder must not stop the server from starting.
      LOGGER.warn(
          "Cannot delete {}, which holds an admin token; delete it yourself", serviceTokenFile, e);
    }
  }
}
