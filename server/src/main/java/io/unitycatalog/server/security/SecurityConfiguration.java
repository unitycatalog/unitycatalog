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
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
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
 * <p>The keys are read from the files set in {@code server.token-signing-private-key-file} and
 * {@code server.token-signing-public-key-file}: a PEM key, or a PKCS #8 (private) or X.509 (public)
 * DER key. Leaving both settings unset is deprecated: the server then uses {@code private_key.der}
 * and {@code public_key.der} in the UC configuration folder, generating them on first start. To
 * create a key pair for the settings: openssl genrsa -out private_key.pem 2048, then openssl rsa
 * -in private_key.pem -pubout -out public_key.pem
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

  private final Path rsa512PrivateKey;
  private final Path rsa512PublicKey;

  /** Uses the key in the configuration folder, generating it if missing. */
  @SneakyThrows
  public SecurityConfiguration(Path configurationFolder) {
    this(
        configurationFolder.resolve("private_key.der"),
        configurationFolder.resolve("public_key.der"));
    initializeIfMissing();
  }

  private SecurityConfiguration(Path privateKeyFile, Path publicKeyFile) {
    rsa512PrivateKey = privateKeyFile;
    rsa512PublicKey = publicKeyFile;
  }

  /**
   * Uses the key files in the settings, or else the key in the configuration folder, generating it
   * if it is missing. Setting only one of the two key files is an error.
   */
  public static SecurityConfiguration load(
      ServerProperties serverProperties, Path configurationFolder) {
    SecurityConfiguration configured = fromSettings(serverProperties);
    if (configured != null) {
      return configured;
    }
    // Without authorization the server checks no tokens, so the key does not matter.
    if (serverProperties.isAuthorizationEnabled()) {
      log.warn(
          "{} and {} are not set, so tokens are signed with a key the server keeps in {}. This is"
              + " deprecated; set both to your own key files.",
          Property.TOKEN_SIGNING_PRIVATE_KEY_FILE.getKey(),
          Property.TOKEN_SIGNING_PUBLIC_KEY_FILE.getKey(),
          configurationFolder);
    }
    return new SecurityConfiguration(configurationFolder);
  }

  // Returns null when neither key file is set.
  private static SecurityConfiguration fromSettings(ServerProperties serverProperties) {
    String privateKeyFile =
        blankToNull(serverProperties.get(Property.TOKEN_SIGNING_PRIVATE_KEY_FILE));
    String publicKeyFile =
        blankToNull(serverProperties.get(Property.TOKEN_SIGNING_PUBLIC_KEY_FILE));
    if (privateKeyFile == null && publicKeyFile == null) {
      return null;
    }
    if (privateKeyFile == null || publicKeyFile == null) {
      throw new IllegalStateException(
          String.format(
              "Set both %s and %s",
              Property.TOKEN_SIGNING_PRIVATE_KEY_FILE.getKey(),
              Property.TOKEN_SIGNING_PUBLIC_KEY_FILE.getKey()));
    }
    return fromKeyFiles(Path.of(privateKeyFile), Path.of(publicKeyFile));
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  /** Uses the given key files, which must exist. Nothing is written. */
  private static SecurityConfiguration fromKeyFiles(Path privateKeyFile, Path publicKeyFile) {
    for (Path file : new Path[] {privateKeyFile, publicKeyFile}) {
      if (!Files.isRegularFile(file)) {
        throw new IllegalStateException("No token signing key file at " + file.toAbsolutePath());
      }
    }
    return new SecurityConfiguration(privateKeyFile, publicKeyFile);
  }

  @SneakyThrows
  public void initializeIfMissing() {
    if (Files.notExists(rsa512PublicKey) || Files.notExists(rsa512PrivateKey)) {
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
    if (Files.notExists(rsa512PublicKey)) {
      log.info("No JWT public signing key present.");
      return null;
    }
    X509EncodedKeySpec spec = new X509EncodedKeySpec(readKeyFile(rsa512PublicKey));
    try {
      return (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException(
          "Cannot read the RSA public key in "
              + rsa512PublicKey.toAbsolutePath()
              + "; write it with `openssl rsa -pubout`",
          e);
    }
  }

  public RSAPrivateKey rsaPrivateKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    if (Files.notExists(rsa512PrivateKey)) {
      log.info("No JWT private signing key present.");
      return null;
    }
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(readKeyFile(rsa512PrivateKey));
    try {
      return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(spec);
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException(
          "Cannot read the RSA private key in "
              + rsa512PrivateKey.toAbsolutePath()
              + "; expected a PEM key, or a PKCS #8 DER one",
          e);
    }
  }

  /**
   * Reads a key file in DER, or the first PEM block in a file. A PKCS #1 private key ({@code BEGIN
   * RSA PRIVATE KEY}, as written by the Helm chart and some {@code openssl genrsa} versions) is
   * wrapped in PKCS #8.
   */
  private static byte[] readKeyFile(Path file) throws IOException {
    byte[] content = Files.readAllBytes(file);
    Matcher pem = PEM_BLOCK.matcher(new String(content, StandardCharsets.US_ASCII));
    if (!pem.find()) {
      return content;
    }
    String type = pem.group(1);
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
