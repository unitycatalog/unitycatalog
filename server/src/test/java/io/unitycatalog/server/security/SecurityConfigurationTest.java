package io.unitycatalog.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
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

class SecurityConfigurationTest {

  @TempDir Path keyDirectory;
  @TempDir Path configurationFolder;

  @Test
  void readsTheOperatorsKeyFiles() throws Exception {
    KeyPair keyPair = newKeyPair();
    Path privateKey =
        Files.write(keyDirectory.resolve("signing.key"), keyPair.getPrivate().getEncoded());
    // The public key in PEM, as `openssl rsa -pubout` writes it.
    Path publicKey =
        Files.writeString(
            keyDirectory.resolve("signing.pub"),
            "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(keyPair.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n");

    SecurityConfiguration configuration =
        SecurityConfiguration.load(settings(privateKey, publicKey), configurationFolder);

    assertThat(configuration.rsaPrivateKey()).isEqualTo(keyPair.getPrivate());
    assertThat(configuration.rsaPublicKey()).isEqualTo(keyPair.getPublic());
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }
  }

  @Test
  void requiresBothKeyFilesSetAndPresent() throws Exception {
    Path privateKey = Files.write(keyDirectory.resolve("signing.key"), new byte[0]);
    for (String setting :
        new String[] {
          "server.token-signing-private-key-file", "server.token-signing-public-key-file"
        }) {
      Properties properties = new Properties();
      properties.setProperty(setting, privateKey.toString());
      assertThatThrownBy(
              () ->
                  SecurityConfiguration.load(new ServerProperties(properties), configurationFolder))
          .hasMessageContaining("Set both");
    }
    assertThatThrownBy(
            () ->
                SecurityConfiguration.load(
                    settings(privateKey, keyDirectory.resolve("missing.pub")), configurationFolder))
        .hasMessageContaining("No token signing key file at");
  }

  @Test
  void loadsAnExistingKeyWithoutGeneratingOne() throws Exception {
    ServerProperties noSettings = new ServerProperties(new Properties());
    assertThatThrownBy(() -> SecurityConfiguration.loadExisting(noSettings, configurationFolder))
        .hasMessageContaining("No token signing key file at");
    try (var files = Files.list(configurationFolder)) {
      assertThat(files).isEmpty();
    }

    // The admin token bin/uc-admin-token prints from the key the server generated.
    new SecurityConfiguration(configurationFolder);
    SecurityConfiguration existing =
        SecurityConfiguration.loadExisting(noSettings, configurationFolder);
    String token =
        SecurityContext.createServiceToken(
            existing.algorithmRSA(), "server", SecurityContext.Issuers.INTERNAL);
    assertThat(
            JWT.require(existing.algorithmRSA())
                .build()
                .verify(token)
                .getClaim(JwtClaim.SUBJECT.key())
                .asString())
        .isEqualTo("admin");
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
    new SecurityConfiguration(configurationFolder);

    assertThat(
            PosixFilePermissions.toString(
                Files.getPosixFilePermissions(configurationFolder.resolve("private_key.der"))))
        .isEqualTo("rw-------");
    assertThat(configurationFolder.resolve("public_key.der")).exists();
  }

  private static ServerProperties settings(Path privateKey, Path publicKey) {
    Properties properties = new Properties();
    properties.setProperty("server.token-signing-private-key-file", privateKey.toString());
    properties.setProperty("server.token-signing-public-key-file", publicKey.toString());
    return new ServerProperties(properties);
  }

  private static KeyPair newKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }
}
