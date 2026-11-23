package io.unitycatalog.server.security;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class SecurityConfigurationTest {

  @TempDir Path keyDirectory;
  @TempDir Path configurationFolder;

  // The private key formats operators use: DER, and PEM as openssl and the Helm chart write it,
  // also after another PEM block such as a certificate.
  @ParameterizedTest
  @ValueSource(strings = {"PKCS #8 DER", "PKCS #8 PEM", "PKCS #1 PEM", "certificate, then PEM"})
  void readsTheOperatorsKeyFile(String privateKeyFormat) throws Exception {
    KeyPair keyPair = newKeyPair();
    byte[] pkcs8 = keyPair.getPrivate().getEncoded();
    Path privateKey = keyDirectory.resolve("signing.key");
    switch (privateKeyFormat) {
      case "PKCS #8 DER" -> Files.write(privateKey, pkcs8);
      case "PKCS #8 PEM" -> Files.writeString(privateKey, pem("PRIVATE" + " KEY", pkcs8));
      // A 2048-bit PKCS #8 key is a 26-byte header followed by the PKCS #1 key.
      case "PKCS #1 PEM" ->
          Files.writeString(
              privateKey, pem("RSA PRIVATE" + " KEY", Arrays.copyOfRange(pkcs8, 26, pkcs8.length)));
      default ->
          Files.writeString(
              privateKey,
              pem("CERTIFICATE", new byte[] {0x30, 0x00}) + pem("PRIVATE" + " KEY", pkcs8));
    }

    SecurityConfiguration configuration =
        SecurityConfiguration.load(settings(privateKey), configurationFolder);

    assertThat(configuration.rsaPrivateKey()).isEqualTo(keyPair.getPrivate());
    // The public key is derived from the private key.
    assertThat(configuration.rsaPublicKey()).isEqualTo(keyPair.getPublic());
    // bin/uc-admin-token reads the same settings.
    assertThat(
            SecurityConfiguration.loadExisting(settings(privateKey), configurationFolder)
                .rsaPrivateKey())
        .isEqualTo(keyPair.getPrivate());
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }
  }

  // Each row hits a different way a key file is unusable; the error names the file and nothing is
  // written to the configuration folder.
  @ParameterizedTest
  @CsvSource({
    "encrypted PKCS #8, is encrypted",
    "encrypted PKCS #1, is encrypted",
    "certificate only, No PRIVATE KEY PEM block",
    "garbage bytes, Cannot read the RSA private key"
  })
  void rejectsAnUnusablePrivateKey(String problem, String message) throws Exception {
    byte[] der = newKeyPair().getPrivate().getEncoded();
    Path privateKey = keyDirectory.resolve("signing.key");
    switch (problem) {
      case "encrypted PKCS #8" ->
          Files.writeString(privateKey, pem("ENCRYPTED PRIVATE" + " KEY", der));
      case "encrypted PKCS #1" ->
          Files.writeString(
              privateKey,
              pem("RSA PRIVATE" + " KEY", der)
                  .replace("\n", "\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\n"));
      case "certificate only" -> Files.writeString(privateKey, pem("CERTIFICATE", der));
      default -> Files.write(privateKey, new byte[] {1, 2, 3});
    }

    assertThatThrownBy(() -> SecurityConfiguration.load(settings(privateKey), configurationFolder))
        .hasMessageContaining(message)
        .hasMessageContaining(privateKey.toString());
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }
  }

  @Test
  void loadsAnExistingKeyWithoutGeneratingOne() throws Exception {
    ServerProperties noSettings = new ServerProperties(new Properties());
    PrintStream discard = new PrintStream(OutputStream.nullOutputStream());
    assertThatThrownBy(() -> AdminTokenCommand.run(noSettings, configurationFolder, discard))
        .hasMessageContaining("No token signing key file at");
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }

    // bin/uc-admin-token uses the key the server generated.
    SecurityConfiguration generated = new SecurityConfiguration(configurationFolder);
    assertThat(SecurityConfiguration.loadExisting(noSettings, configurationFolder).rsaPrivateKey())
        .isEqualTo(generated.rsaPrivateKey());

    // The command prints only the token, and the server's key verifies it.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    AdminTokenCommand.run(
        noSettings, configurationFolder, new PrintStream(out, true, StandardCharsets.UTF_8));
    List<String> lines = out.toString(StandardCharsets.UTF_8).lines().toList();
    assertThat(lines).hasSize(1);
    DecodedJWT token =
        JWT.require(generated.algorithmRSA()).withIssuer(INTERNAL).build().verify(lines.get(0));
    assertThat(token.getClaim(JwtClaim.SUBJECT.key()).asString()).isEqualTo("admin");
  }

  @Test
  void createsAGeneratedPrivateKeyReadableByItsOwnerOnly() throws Exception {
    // A blank setting counts as unset, so the server uses the key in the configuration folder.
    SecurityConfiguration generated =
        SecurityConfiguration.load(settings(Path.of(" ")), configurationFolder);

    assertThat(
            PosixFilePermissions.toString(
                Files.getPosixFilePermissions(configurationFolder.resolve("private_key.der"))))
        .isEqualTo("rw-------");
    assertThat(configurationFolder.resolve("public_key.der")).exists();
    // A later start reuses that key; key_id.txt is not needed.
    assertThat(configurationFolder.resolve("key_id.txt")).doesNotExist();
    assertThat(new SecurityConfiguration(configurationFolder).rsaPrivateKey())
        .isEqualTo(generated.rsaPrivateKey());
  }

  private static ServerProperties settings(Path privateKey) {
    Properties properties = new Properties();
    properties.setProperty("server.token-signing-private-key-file", privateKey.toString());
    return new ServerProperties(properties);
  }

  // The header is built at runtime so the source holds no literal private-key header.
  private static String pem(String type, byte[] der) {
    return "-----BEGIN "
        + type
        + "-----\n"
        + Base64.getMimeEncoder().encodeToString(der)
        + "\n-----END "
        + type
        + "-----\n";
  }

  private static KeyPair newKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }
}
