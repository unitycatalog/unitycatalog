package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CredentialCipherTest {

  private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);
  private static final String OTHER_KEY =
      Base64.getEncoder().encodeToString("another 32-byte credential key!!".getBytes());

  @TempDir Path directory;

  @Test
  void readsTheKeyFromThePropertyOrAFile() throws Exception {
    Path keyFile = Files.writeString(directory.resolve("credential.key"), KEY + "\n");
    String stored = cipher("server.credential-encryption-key", KEY).convertToDatabaseColumn("{}");

    assertThat(stored).startsWith(CredentialCipher.PREFIX);
    assertThat(
            cipher("server.credential-encryption-key-file", keyFile.toString())
                .convertToEntityAttribute(stored))
        .isEqualTo("{}");
    // Without a key, values are stored as they are.
    assertThat(
            CredentialCipher.from(new ServerProperties(new Properties()))
                .convertToDatabaseColumn("{}"))
        .isEqualTo("{}");
  }

  @Test
  void rejectsBothKeySettingsAndAKeyThatIsNot32Bytes() {
    Properties both = new Properties();
    both.setProperty("server.credential-encryption-key", KEY);
    both.setProperty("server.credential-encryption-key-file", "/unused");
    assertThatThrownBy(() -> CredentialCipher.from(new ServerProperties(both)))
        .hasMessageContaining("Set only one of");
    assertThatThrownBy(() -> cipher("server.credential-encryption-key", "c2hvcnQ="))
        .hasMessageContaining("32 bytes");
    assertThatThrownBy(() -> cipher("server.credential-encryption-key", "a passphrase"))
        .hasMessageContaining("not valid base64");
  }

  @Test
  void failsToReadAnEncryptedValueWithAnotherKeyOrNoKey() {
    String stored = cipher("server.credential-encryption-key", KEY).convertToDatabaseColumn("{}");

    assertThatThrownBy(
            () ->
                cipher("server.credential-encryption-key", OTHER_KEY)
                    .convertToEntityAttribute(stored))
        .hasMessageContaining("key may be wrong");
    assertThatThrownBy(() -> new CredentialCipher(null).convertToEntityAttribute(stored))
        .hasMessageContaining("no credential encryption key is set");
  }

  private static CredentialCipher cipher(String key, String value) {
    Properties properties = new Properties();
    properties.setProperty(key, value);
    return CredentialCipher.from(new ServerProperties(properties));
  }
}
