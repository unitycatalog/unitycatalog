package io.unitycatalog.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecurityConfigurationTest {

  @TempDir Path keyDirectory;

  @Test
  void readsTheOperatorsKeyAndDerivesThePublicKey() throws Exception {
    KeyPair keyPair = newKeyPair();
    Files.write(keyDirectory.resolve("private_key.der"), keyPair.getPrivate().getEncoded());
    Files.writeString(keyDirectory.resolve("key_id.txt"), "operator-key\n");
    // A public key left from another key pair is not used.
    Files.write(keyDirectory.resolve("public_key.der"), newKeyPair().getPublic().getEncoded());

    SecurityConfiguration configuration = SecurityConfiguration.fromKeyDirectory(keyDirectory);

    assertThat(configuration.rsaPublicKey()).isEqualTo(keyPair.getPublic());
    assertThat(configuration.getKeyId()).isEqualTo("operator-key");
    try (var files = Files.list(keyDirectory)) {
      assertThat(files.map(path -> path.getFileName().toString()))
          .containsExactlyInAnyOrder("private_key.der", "public_key.der", "key_id.txt");
    }
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
  void rejectsAKeyDirectoryWithoutKeyId() throws Exception {
    Files.write(keyDirectory.resolve("private_key.der"), newKeyPair().getPrivate().getEncoded());

    assertThatThrownBy(() -> SecurityConfiguration.fromKeyDirectory(keyDirectory))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("server.token-signing-key-dir")
        .hasMessageContaining("key_id.txt");
  }

  @Test
  void generatesAPrivateKeyOnlyItsOwnerCanRead() throws Exception {
    new SecurityConfiguration(keyDirectory);

    assertThat(Files.getPosixFilePermissions(keyDirectory.resolve("private_key.der")))
        .isEqualTo(PosixFilePermissions.fromString("rw-------"));
  }

  private static KeyPair newKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }
}
