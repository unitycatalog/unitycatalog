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
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RSA key pair the server signs and checks its own tokens with.
 *
 * <p>The private key is read from the file set in {@code server.token-signing-private-key-file}: an
 * unencrypted PEM key (PKCS #8 or PKCS #1), or a PKCS #8 DER key. The public key is derived from it
 * once, when the configuration is loaded. Leaving the setting unset is deprecated: the server then
 * uses {@code private_key.der} and {@code public_key.der} in the UC configuration folder,
 * generating them on first start.
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

  private static final Pattern PEM_BLOCK =
      Pattern.compile("-----BEGIN ([A-Z ]+)-----(.*?)-----END \\1-----", Pattern.DOTALL);

  private final RSAPrivateKey privateKey;
  private final RSAPublicKey publicKey;

  /** Uses the key in the configuration folder, generating it if missing. */
  @SneakyThrows
  public SecurityConfiguration(Path configurationFolder) {
    Path privateKeyFile = configurationFolder.resolve("private_key.der");
    Path publicKeyFile = configurationFolder.resolve("public_key.der");
    initializeIfMissing(privateKeyFile, publicKeyFile);
    privateKey = readPrivateKey(privateKeyFile);
    publicKey = readPublicKey(publicKeyFile);
  }

  private SecurityConfiguration(RSAPrivateKey privateKey, RSAPublicKey publicKey) {
    this.privateKey = privateKey;
    this.publicKey = publicKey;
  }

  /**
   * Uses the private key file in the settings, or else the key in the configuration folder,
   * generating it if it is missing. A key file that is set must exist.
   */
  public static SecurityConfiguration load(
      ServerProperties serverProperties, Path configurationFolder) {
    SecurityConfiguration configured = fromSettings(serverProperties);
    if (configured != null) {
      return configured;
    }
    // Without authorization no request checks the server's own tokens, so the key does not matter.
    if (serverProperties.isAuthorizationEnabled()) {
      log.warn(
          "{} is not set, so tokens are signed with a key the server keeps in {}. This is"
              + " deprecated; set it to your own private key file.",
          Property.TOKEN_SIGNING_PRIVATE_KEY_FILE.getKey(),
          configurationFolder);
    }
    return new SecurityConfiguration(configurationFolder);
  }

  // Returns null when the key file is not set.
  private static SecurityConfiguration fromSettings(ServerProperties serverProperties) {
    String privateKeyFile = serverProperties.get(Property.TOKEN_SIGNING_PRIVATE_KEY_FILE);
    if (privateKeyFile == null || privateKeyFile.isBlank()) {
      return null;
    }
    return fromPrivateKeyFile(Path.of(privateKeyFile));
  }

  /** Uses the given private key file and the public key derived from it. Nothing is written. */
  private static SecurityConfiguration fromPrivateKeyFile(Path privateKeyFile) {
    requireFile(privateKeyFile);
    RSAPrivateKey privateKey = readPrivateKey(privateKeyFile);
    return new SecurityConfiguration(privateKey, publicKeyOf(privateKey, privateKeyFile));
  }

  // A file that exists but cannot be read is reported by the read, with the real cause.
  private static void requireFile(Path file) {
    if (Files.notExists(file)) {
      throw new IllegalStateException("No token signing key file at " + file.toAbsolutePath());
    }
  }

  @SneakyThrows
  private static RSAPublicKey publicKeyOf(RSAPrivateKey privateKey, Path file) {
    if (!(privateKey instanceof RSAPrivateCrtKey crtKey)) {
      throw new IllegalStateException(
          "The key in "
              + file.toAbsolutePath()
              + " has no public exponent; create it with"
              + " `openssl genrsa`");
    }
    return (RSAPublicKey)
        KeyFactory.getInstance("RSA")
            .generatePublic(new RSAPublicKeySpec(crtKey.getModulus(), crtKey.getPublicExponent()));
  }

  @SneakyThrows
  private static void initializeIfMissing(Path privateKeyFile, Path publicKeyFile) {
    if (Files.notExists(publicKeyFile) || Files.notExists(privateKeyFile)) {
      log.info("Initializing security configuration.");
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      // Create parent directory first if it does not exist
      Path parentDirectory = publicKeyFile.getParent();
      if (parentDirectory != null && !Files.exists(parentDirectory)) {
        Files.createDirectories(parentDirectory);
      }
      Files.write(publicKeyFile, keyPair.getPublic().getEncoded(), StandardOpenOption.CREATE);
      writeOwnerOnly(privateKeyFile, keyPair.getPrivate().getEncoded());
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

  public Algorithm algorithmRSA() {
    return Algorithm.RSA512(publicKey, privateKey);
  }

  public RSAPublicKey rsaPublicKey() {
    return publicKey;
  }

  public RSAPrivateKey rsaPrivateKey() {
    return privateKey;
  }

  @SneakyThrows
  private static RSAPublicKey readPublicKey(Path file) {
    X509EncodedKeySpec spec = new X509EncodedKeySpec(readKeyFile(file, "PUBLIC KEY"));
    try {
      return (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException(
          "Cannot read the RSA public key in " + file.toAbsolutePath(), e);
    }
  }

  @SneakyThrows
  private static RSAPrivateKey readPrivateKey(Path file) {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(readKeyFile(file, "PRIVATE KEY"));
    try {
      return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(spec);
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException(
          "Cannot read the RSA private key in "
              + file.toAbsolutePath()
              + "; expected a PEM key, or a PKCS #8 DER one",
          e);
    }
  }

  /**
   * Reads a key file in DER, or the first PEM block of the given kind ({@code PRIVATE KEY} or
   * {@code PUBLIC KEY}), skipping others such as a certificate. A PKCS #1 private key ({@code BEGIN
   * RSA PRIVATE KEY}, as written by the Helm chart and some {@code openssl genrsa} versions) is
   * wrapped in PKCS #8.
   */
  private static byte[] readKeyFile(Path file, String kind) throws IOException {
    byte[] content = Files.readAllBytes(file);
    Matcher pem = PEM_BLOCK.matcher(new String(content, StandardCharsets.US_ASCII));
    boolean foundPem = false;
    while (pem.find()) {
      foundPem = true;
      String type = pem.group(1);
      if (!type.endsWith(kind)) {
        continue;
      }
      if (type.contains("ENCRYPTED") || pem.group(2).contains("Proc-Type:")) {
        throw new IllegalStateException(
            "The key in "
                + file.toAbsolutePath()
                + " is encrypted; give the server an unencrypted key");
      }
      byte[] der;
      try {
        der = Base64.getMimeDecoder().decode(pem.group(2));
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException("Cannot read the PEM key in " + file.toAbsolutePath(), e);
      }
      return type.equals("RSA PRIVATE KEY") ? pkcs1ToPkcs8(der) : der;
    }
    if (foundPem) {
      throw new IllegalStateException("No " + kind + " PEM block in " + file.toAbsolutePath());
    }
    return content;
  }

  /** Wraps a PKCS #1 RSA private key in a PKCS #8 PrivateKeyInfo structure. */
  private static byte[] pkcs1ToPkcs8(byte[] pkcs1) {
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
