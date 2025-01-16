package io.unitycatalog.server.security;

import com.auth0.jwt.algorithms.Algorithm;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Security settings for the Authnz framework.
 *
 * <p>The settings are loaded from files persisted in the UC configuration folder. If no settings
 * files exist, they will be generated for reuse across restarts. Equivalent openssl commands are -
 * openssl genrsa -out private_key.pem 2048 - openssl rsa -in private_key.pem -pubout -outform DER
 * -out public_key.der - openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem -out
 * private_key.der -nocrypt - openssl rand -hex -out key_id.txt 32
 */
public class SecurityConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SecurityConfiguration.class);

  private Path rsa512PublicKey;

  private Path rsa512PrivateKey;

  private Path keyId;

  @SneakyThrows
  public SecurityConfiguration(Path configurationFolder) {
    rsa512PublicKey = configurationFolder.resolve("public_key.der");
    rsa512PrivateKey = configurationFolder.resolve("private_key.der");
    keyId = configurationFolder.resolve("key_id.txt");

    initializeIfMissing();
  }

  @SneakyThrows
  public void initializeIfMissing() {
    if (Files.notExists(rsa512PublicKey)
        || Files.notExists(rsa512PrivateKey)
        || Files.notExists(keyId)) {
      log.info("Initializing security configuration.");
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      // Create parent directory first if it does not exist
      Path parentDirectory = rsa512PublicKey.getParent();
      if (parentDirectory != null && !Files.exists(parentDirectory)) {
        Files.createDirectories(parentDirectory);
      }
      Files.write(rsa512PublicKey, keyPair.getPublic().getEncoded(), StandardOpenOption.CREATE);
      Files.write(rsa512PrivateKey, keyPair.getPrivate().getEncoded(), StandardOpenOption.CREATE);

      byte[] keyIdBytes = new byte[32];
      new SecureRandom().nextBytes(keyIdBytes);
      Files.writeString(keyId, Hex.encodeHexString(keyIdBytes), StandardOpenOption.CREATE);
    }
  }

  public Algorithm algorithmRSA()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    return Algorithm.RSA512(rsaPublicKey(), rsaPrivateKey());
  }

  public RSAPublicKey rsaPublicKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {

    if (Files.notExists(rsa512PublicKey)) {
      log.info("No JWT public signing key present.");
      return null;
    }
    byte[] keyBytes = Files.readAllBytes(rsa512PublicKey);

    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return (RSAPublicKey) kf.generatePublic(spec);
  }

  public RSAPrivateKey rsaPrivateKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {

    if (Files.notExists(rsa512PrivateKey)) {
      log.info("No JWT private signing key present.");
      return null;
    }
    byte[] keyBytes = Files.readAllBytes(rsa512PrivateKey);

    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return (RSAPrivateKey) kf.generatePrivate(spec);
  }

  public String getKeyId() throws IOException {
    return Files.readString(keyId).trim();
  }
}
