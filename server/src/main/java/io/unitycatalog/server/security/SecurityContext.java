package io.unitycatalog.server.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityContext {

  public interface Issuers {
    String INTERNAL = "internal";
  }

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(SecurityContext.class);

  @Getter private final Path certsFile;
  private final Path serviceTokenFile;

  @Getter private final RSAPublicKey rsaPublicKey;
  @Getter private final RSAPrivateKey rsaPrivateKey;
  @Getter private final Algorithm algorithm;
  @Getter private final String serviceToken;
  @Getter private final String keyId;
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

    certsFile = configurationFolder.resolve("certs.json");
    serviceTokenFile = configurationFolder.resolve("token.txt");

    rsaPublicKey = securityConfiguration.rsaPublicKey();
    rsaPrivateKey = securityConfiguration.rsaPrivateKey();
    algorithm = securityConfiguration.algorithmRSA();
    keyId = securityConfiguration.getKeyId();

    // Do this every time because the SecurityConfiguration parameters
    // might have changed since last started.
    serviceToken = createServiceToken();

    createInternalCertsFile();
    deleteServiceTokenFile();

    LOGGER.info("--- Internal Certs Configuration --");
    LOGGER.info(getInternalCertsFile());
  }

  public String createAccessToken(String principalEmail, Duration ttl) {
    Instant expiresAt = Instant.now().plus(ttl);
    return JWT.create()
        .withSubject(serviceName)
        .withIssuer(localIssuer)
        .withIssuedAt(new Date())
        .withExpiresAt(Date.from(expiresAt))
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.ACCESS.name())
        .withClaim(JwtClaim.SUBJECT.key(), principalEmail)
        .sign(algorithm);
  }

  public String createServiceToken() {
    return createServiceToken(algorithm, keyId, serviceName, localIssuer);
  }

  /** Creates an admin service token. It has no expiry, so it is valid while the key is. */
  static String createServiceToken(
      Algorithm algorithm, String keyId, String serviceName, String localIssuer) {
    return JWT.create()
        .withSubject(serviceName)
        .withIssuer(localIssuer)
        .withIssuedAt(new Date())
        .withKeyId(keyId)
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(JwtClaim.TOKEN_TYPE.key(), JwtTokenType.SERVICE.name())
        .withClaim(JwtClaim.SUBJECT.key(), "admin")
        .sign(algorithm);
  }

  @SneakyThrows
  public void createInternalCertsFile() {
    Map<String, Object> key = new LinkedHashMap<>();
    key.put("kid", keyId);
    key.put("use", "sig");
    key.put("kty", rsaPublicKey.getAlgorithm());
    key.put("alg", algorithm.getName());
    key.put(
        "e", Base64.getUrlEncoder().encodeToString(rsaPublicKey.getPublicExponent().toByteArray()));
    key.put("n", Base64.getUrlEncoder().encodeToString(rsaPublicKey.getModulus().toByteArray()));

    List<Map> keyList = new ArrayList<>();
    keyList.add(key);

    Map<String, Object> keyMap = new HashMap<>();
    keyMap.put("keys", keyList);
    Files.writeString(
        certsFile, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(keyMap));
  }

  @SneakyThrows
  public String getInternalCertsFile() {
    return Files.readString(certsFile);
  }

  // The admin token stays in memory; bin/uc-admin-token prints one. A token.txt in the
  // configuration folder is a plaintext copy of an admin token that does not expire, so remove it.
  // Removing the file does not revoke copies; changing the signing key does.
  @SneakyThrows
  private void deleteServiceTokenFile() {
    if (Files.deleteIfExists(serviceTokenFile)) {
      LOGGER.warn(
          "Deleted {}: the server keeps the admin token in memory, and bin/uc-admin-token prints"
              + " one. Copies of the deleted token stay valid until the signing key changes.",
          serviceTokenFile);
    }
  }
}
