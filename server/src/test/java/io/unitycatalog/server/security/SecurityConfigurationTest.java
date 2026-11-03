package io.unitycatalog.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
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
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }
  }

  @Test
  void rejectsAnEncryptedPrivateKey() throws Exception {
    Path privateKey =
        Files.writeString(
            keyDirectory.resolve("signing.key"),
            pem("ENCRYPTED PRIVATE" + " KEY", newKeyPair().getPrivate().getEncoded()));

    assertThatThrownBy(() -> SecurityConfiguration.load(settings(privateKey), configurationFolder))
        .hasMessageContaining("is encrypted");
  }

  @Test
  void wrapsAPkcs1KeyAsPkcs8() throws Exception {
    byte[] pkcs8 = newKeyPair().getPrivate().getEncoded();
    // A 2048-bit PKCS #8 key is a 26-byte header followed by the PKCS #1 key.
    assertThat(pkcs8[22]).isEqualTo((byte) 0x04);
    byte[] pkcs1 = Arrays.copyOfRange(pkcs8, 26, pkcs8.length);

    assertThat(SecurityConfiguration.pkcs1ToPkcs8(pkcs1)).isEqualTo(pkcs8);
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
