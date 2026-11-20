package io.unitycatalog.server.persist.secret;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.secret.CredentialSecretStore.SecretContext;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.AEADBadTagException;
import org.junit.jupiter.api.Test;

public class EncryptedCredentialSecretStoreTest {
  private static final UUID CREDENTIAL_ID = UUID.randomUUID();
  private static final SecretContext CONTEXT =
      new SecretContext(CREDENTIAL_ID, "credential", "field");
  private static final EncryptedCredentialSecretStore STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  private static void assertCannotLoad(
      EncryptedCredentialSecretStore store, String stored, SecretContext context, String reason) {
    assertThatThrownBy(() -> store.load(stored, context))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(reason)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
  }

  @Test
  public void testValueLoadsOnlyWithItsKeyAndCredential() {
    String stored = STORE.store("value-1", CONTEXT);

    assertThat(stored).startsWith("v1:").doesNotContain("value-1");
    // A fresh nonce each time, so equal values do not look equal in the database.
    assertThat(STORE.store("value-1", CONTEXT)).isNotEqualTo(stored);
    assertThat(STORE.load(stored, CONTEXT)).isEqualTo("value-1");
    // A renamed credential keeps its stored value.
    assertThat(STORE.load(stored, new SecretContext(CREDENTIAL_ID, "renamed", "field")))
        .isEqualTo("value-1");

    // Copied to another credential or field.
    for (SecretContext other :
        List.of(
            new SecretContext(UUID.randomUUID(), "credential", "field"),
            new SecretContext(CREDENTIAL_ID, "credential", "other_field"))) {
      assertCannotLoad(STORE, stored, other, "does not decrypt");
    }
    // Changed in the database.
    String[] parts = stored.split(":", 3);
    byte[] payload = Base64.getDecoder().decode(parts[2]);
    payload[payload.length - 1] ^= 1;
    String changed = parts[0] + ":" + parts[1] + ":" + Base64.getEncoder().encodeToString(payload);
    assertCannotLoad(STORE, changed, CONTEXT, "does not decrypt");
    // The failure keeps its cause for the logs.
    assertThatThrownBy(() -> STORE.load(changed, CONTEXT))
        .hasCauseInstanceOf(AEADBadTagException.class);
    // Read by a server with another key, or none.
    EncryptedCredentialSecretStore otherKey =
        new EncryptedCredentialSecretStore(
            Optional.of(Base64.getEncoder().encodeToString(new byte[32])));
    assertCannotLoad(otherKey, stored, CONTEXT, "encrypted with key " + parts[1]);
    assertCannotLoad(
        new EncryptedCredentialSecretStore(Optional.empty()),
        stored,
        CONTEXT,
        "no server.credential-encryption-key");
    // Not written by this store.
    assertCannotLoad(STORE, "value-1", CONTEXT, "not in a known format");
  }

  @Test
  public void testMalformedPayloadIsRefused() {
    String keyId = STORE.store("value-1", CONTEXT).split(":", 3)[1];
    assertCannotLoad(STORE, "v1:" + keyId + ":not base64!", CONTEXT, "does not decrypt");
    // A payload shorter than a nonce and a tag (12 + 16 bytes) cannot hold a value.
    for (int length : List.of(0, 11, 12, 27)) {
      String stored = "v1:" + keyId + ":" + Base64.getEncoder().encodeToString(new byte[length]);
      assertCannotLoad(STORE, stored, CONTEXT, "too short to decrypt");
    }
    String noValue = "v1:" + keyId + ":" + Base64.getEncoder().encodeToString(new byte[28]);
    assertCannotLoad(STORE, noValue, CONTEXT, "does not decrypt");
  }

  @Test
  public void testV1ValueStoredEarlierStillReads() {
    // Encrypted outside this class with the v1 format: its binding is "storage credential <id>
    // <field>". A value stored by an earlier server must read whatever the error messages say.
    SecretContext context =
        new SecretContext(
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            "credential",
            "secret_access_key");
    assertThat(STORE.load("v1:3eb1bd43:AAECAwQFBgcICQoLW4TXCbw1pJ1y8P5fmnP7SG2nSM0dT2uH", context))
        .isEqualTo("value-v1");
  }

  @Test
  public void testServerWithoutKeyRefusesToStore() {
    assertThatThrownBy(
            () -> new EncryptedCredentialSecretStore(Optional.empty()).store("value-1", CONTEXT))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("UC_SERVER_CREDENTIAL_ENCRYPTION_KEY")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);
  }

  @Test
  public void testKeyIsReadOnlyWithS3CompatibleStorageEnabled() {
    for (boolean enabled : List.of(true, false)) {
      Properties properties = new Properties();
      properties.setProperty(
          Property.S3_COMPATIBLE_STORAGE_ENABLED.getKey(), String.valueOf(enabled));
      properties.setProperty(Property.CREDENTIAL_ENCRYPTION_KEY.getKey(), "not base64!");
      ServerProperties serverProperties = new ServerProperties(properties);
      if (enabled) {
        // A malformed key stops the server only when the feature that uses it is on.
        assertThatThrownBy(() -> EncryptedCredentialSecretStore.from(serverProperties))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be 32 bytes encoded in base64");
      } else {
        assertThatThrownBy(
                () -> EncryptedCredentialSecretStore.from(serverProperties).store("v", CONTEXT))
            .isInstanceOf(BaseException.class)
            .extracting(e -> ((BaseException) e).getErrorCode())
            .isEqualTo(ErrorCode.FAILED_PRECONDITION);
      }
    }
  }

  @Test
  public void testKeyMustBe256BitsInBase64() {
    for (String key : List.of("not base64!", Base64.getEncoder().encodeToString(new byte[16]))) {
      assertThatThrownBy(() -> new EncryptedCredentialSecretStore(Optional.of(key)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be 32 bytes encoded in base64")
          .hasMessageNotContaining(key);
    }
  }
}
