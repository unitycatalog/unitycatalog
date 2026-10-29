package io.unitycatalog.server.security;

import com.auth0.jwt.algorithms.Algorithm;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Security settings for the Authnz framework.
 *
 * <p>The settings are loaded from the directory set in {@code server.token-signing-key-dir}, which
 * holds {@code private_key.der} or {@code private_key.pem}, and {@code key_id.txt}. Without that
 * setting they are loaded from files persisted in the UC configuration folder, and generated there
 * for reuse across restarts if they do not exist. Equivalent openssl commands are - openssl genrsa
 * -out private_key.pem 2048 - openssl rsa -in private_key.pem -pubout -outform DER -out
 * public_key.der - openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem -out
 * private_key.der -nocrypt - openssl rand -hex -out key_id.txt 32
 */
public class SecurityConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SecurityConfiguration.class);

  // DER AlgorithmIdentifier for rsaEncryption (OID 1.2.840.113549.1.1.1) with NULL parameters.
  private static final byte[] RSA_ALGORITHM_IDENTIFIER = {
    0x30,
    0x0d,
    0x06,
    0x09,
    0x2a,
    (byte) 0x86,
    0x48,
    (byte) 0x86,
    (byte) 0xf7,
    0x0d,
    0x01,
    0x01,
    0x01,
    0x05,
    0x00
  };

  private final Path rsa512PublicKey;
  private final Path rsa512PrivateKey;
  private final Path keyId;

  /** Uses the key in the configuration folder, and generates it there if it is missing. */
  @SneakyThrows
  public SecurityConfiguration(Path configurationFolder) {
    this(configurationFolder, "private_key.der");
    log.warn(
        "{} is not set, so tokens are signed with a key the server keeps in {}. Set it to a"
            + " directory holding your own private_key.der or private_key.pem, and key_id.txt.",
        Property.TOKEN_SIGNING_KEY_DIR.getKey(),
        configurationFolder);
    initializeIfMissing();
  }

  private SecurityConfiguration(Path folder, String privateKeyFileName) {
    rsa512PublicKey = folder.resolve("public_key.der");
    rsa512PrivateKey = folder.resolve(privateKeyFileName);
    keyId = folder.resolve("key_id.txt");
  }

  /**
   * Uses the key directory set in {@code server.token-signing-key-dir}, or else the key in the
   * configuration folder.
   */
  public static SecurityConfiguration load(
      ServerProperties serverProperties, Path configurationFolder) {
    String keyDirectory = serverProperties.get(Property.TOKEN_SIGNING_KEY_DIR);
    return keyDirectory != null
        ? fromKeyDirectory(Path.of(keyDirectory))
        : new SecurityConfiguration(configurationFolder);
  }

  /**
   * Uses the operator's key in {@code keyDirectory}, which must hold {@code private_key.der} (PKCS
   * #8) or {@code private_key.pem} (PKCS #8 or PKCS #1), and {@code key_id.txt}. The public key is
   * derived from the private key. Nothing is written there.
   */
  static SecurityConfiguration fromKeyDirectory(Path keyDirectory) {
    String privateKeyFileName =
        Files.exists(keyDirectory.resolve("private_key.der"))
            ? "private_key.der"
            : "private_key.pem";
    SecurityConfiguration configuration =
        new SecurityConfiguration(keyDirectory, privateKeyFileName);
    if (Files.notExists(configuration.rsa512PrivateKey) || Files.notExists(configuration.keyId)) {
      throw new IllegalStateException(
          String.format(
              "No token signing key in %s: it must hold private_key.der or private_key.pem, and"
                  + " key_id.txt (see %s)",
              keyDirectory.toAbsolutePath(), Property.TOKEN_SIGNING_KEY_DIR.getKey()));
    }
    return configuration;
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
      writeOwnerOnly(rsa512PrivateKey, keyPair.getPrivate().getEncoded());

      byte[] keyIdBytes = new byte[32];
      new SecureRandom().nextBytes(keyIdBytes);
      Files.writeString(keyId, Hex.encodeHexString(keyIdBytes), StandardOpenOption.CREATE);
    }
  }

  // Creates the file readable and writable by its owner only, where the file system supports it.
  private static void writeOwnerOnly(Path file, byte[] content) throws IOException {
    Files.deleteIfExists(file);
    if (file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      Files.createFile(
          file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    }
    Files.write(file, content);
  }

  public Algorithm algorithmRSA()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    return Algorithm.RSA512(rsaPublicKey(), rsaPrivateKey());
  }

  public RSAPublicKey rsaPublicKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    KeyFactory kf = KeyFactory.getInstance("RSA");
    // Derived from the private key, so it always matches the key that signs the tokens.
    if (rsaPrivateKey() instanceof RSAPrivateCrtKey privateKey) {
      return (RSAPublicKey)
          kf.generatePublic(
              new RSAPublicKeySpec(privateKey.getModulus(), privateKey.getPublicExponent()));
    }
    if (Files.notExists(rsa512PublicKey)) {
      log.info("No JWT public signing key present.");
      return null;
    }
    byte[] keyBytes = Files.readAllBytes(rsa512PublicKey);

    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    return (RSAPublicKey) kf.generatePublic(spec);
  }

  public RSAPrivateKey rsaPrivateKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    if (Files.notExists(rsa512PrivateKey)) {
      log.info("No JWT private signing key present.");
      return null;
    }
    byte[] keyBytes = Files.readAllBytes(rsa512PrivateKey);
    if (rsa512PrivateKey.getFileName().toString().endsWith(".pem")) {
      keyBytes = pemToPkcs8(new String(keyBytes, StandardCharsets.US_ASCII));
    }

    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return (RSAPrivateKey) kf.generatePrivate(spec);
  }

  public String getKeyId() throws IOException {
    return Files.readString(keyId).trim();
  }

  /**
   * Decodes a PEM private key to PKCS #8. A PKCS #1 key ({@code BEGIN RSA PRIVATE KEY}, as written
   * by {@code openssl genrsa -traditional} and the Helm chart) is wrapped in a PKCS #8 structure.
   */
  private static byte[] pemToPkcs8(String pem) {
    byte[] der =
        Base64.getMimeDecoder().decode(pem.replaceAll("-----(BEGIN|END) [A-Z ]+-----", ""));
    return pem.contains("BEGIN RSA PRIVATE KEY") ? pkcs1ToPkcs8(der) : der;
  }

  /** Wraps a PKCS #1 RSA private key in a PKCS #8 PrivateKeyInfo structure. */
  static byte[] pkcs1ToPkcs8(byte[] pkcs1) {
    ByteArrayOutputStream privateKeyInfo = new ByteArrayOutputStream();
    privateKeyInfo.writeBytes(new byte[] {0x02, 0x01, 0x00}); // version 0
    privateKeyInfo.writeBytes(RSA_ALGORITHM_IDENTIFIER);
    privateKeyInfo.writeBytes(derElement(0x04, pkcs1));
    return derElement(0x30, privateKeyInfo.toByteArray());
  }

  private static byte[] derElement(int tag, byte[] content) {
    ByteArrayOutputStream element = new ByteArrayOutputStream();
    element.write(tag);
    if (content.length < 0x80) {
      element.write(content.length);
    } else {
      int lengthBytes = (Integer.SIZE - Integer.numberOfLeadingZeros(content.length) + 7) / 8;
      element.write(0x80 | lengthBytes);
      for (int i = lengthBytes - 1; i >= 0; i--) {
        element.write(content.length >>> (8 * i));
      }
    }
    element.writeBytes(content);
    return element.toByteArray();
  }
}
