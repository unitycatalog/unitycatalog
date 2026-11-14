package io.unitycatalog.server.persist.secret;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Keeps secrets in the database, encrypted with AES-256-GCM and the key set in {@code
 * server.credential-encryption-key}, so the database alone does not reveal them.
 *
 * <p>A stored value is {@code v1:<key id>:<base64 of nonce and ciphertext>}. The key id is derived
 * from the key, so a server started with another key reports which key a value needs. Each value is
 * bound to its credential id and field, so a value copied to another row or field does not decrypt.
 */
public class EncryptedCredentialSecretStore implements CredentialSecretStore {
  private static final String VERSION = "v1";
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int KEY_BYTES = 32;
  private static final int NONCE_BYTES = 12;
  private static final int TAG_BITS = 128;
  private static final SecureRandom RANDOM = new SecureRandom();

  private final Optional<SecretKey> key;
  private final Optional<String> keyId;

  /**
   * @param base64Key the base64-encoded 256-bit key, or empty if none is configured
   * @throws IllegalArgumentException if the key is not a base64-encoded 256-bit key
   */
  public EncryptedCredentialSecretStore(Optional<String> base64Key) {
    this.key = base64Key.map(EncryptedCredentialSecretStore::parseKey);
    this.keyId = key.map(EncryptedCredentialSecretStore::keyIdOf);
  }

  /**
   * The store with the key the server is configured with. The key is read only when {@code
   * server.s3-compatible-storage.enabled} is true, the only feature that uses it, so a malformed
   * key stops the server only then.
   *
   * @throws IllegalArgumentException if the feature is on and the key is not a base64-encoded
   *     256-bit key
   */
  public static EncryptedCredentialSecretStore from(ServerProperties serverProperties) {
    return new EncryptedCredentialSecretStore(
        serverProperties.isS3CompatibleStorageEnabled()
            ? Optional.ofNullable(serverProperties.get(Property.CREDENTIAL_ENCRYPTION_KEY))
            : Optional.empty());
  }

  private static SecretKey parseKey(String base64Key) {
    byte[] bytes;
    try {
      bytes = Base64.getDecoder().decode(base64Key.trim());
    } catch (IllegalArgumentException e) {
      bytes = new byte[0];
    }
    if (bytes.length != KEY_BYTES) {
      // The value itself is not part of the message.
      throw new IllegalArgumentException(
          Property.CREDENTIAL_ENCRYPTION_KEY.getKey()
              + " must be 32 bytes encoded in base64, such as the output of"
              + " `openssl rand -base64 32`.");
    }
    return new SecretKeySpec(bytes, "AES");
  }

  private static String keyIdOf(SecretKey key) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(key.getEncoded());
      return HexFormat.of().formatHex(digest, 0, 4);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The associated data the ciphertext is bound to. The name is left out, since a credential keeps
   * its stored value when it is renamed.
   */
  private static byte[] associatedData(SecretContext context) {
    return describe(context).getBytes(StandardCharsets.UTF_8);
  }

  private static String describe(SecretContext context) {
    return "storage credential " + context.credentialId() + " " + context.field();
  }

  /**
   * @throws BaseException if the server has no key configured
   */
  @Override
  public String store(String secret, SecretContext context) {
    SecretKey secretKey =
        key.orElseThrow(
            () ->
                new BaseException(
                    ErrorCode.FAILED_PRECONDITION,
                    "The server has no "
                        + Property.CREDENTIAL_ENCRYPTION_KEY.getKey()
                        + " to store this credential with; set it in the "
                        + ServerProperties.envName(Property.CREDENTIAL_ENCRYPTION_KEY.getKey())
                        + " environment variable."));
    byte[] nonce = new byte[NONCE_BYTES];
    RANDOM.nextBytes(nonce);
    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(associatedData(context));
      byte[] ciphertext = cipher.doFinal(secret.getBytes(StandardCharsets.UTF_8));
      byte[] payload =
          ByteBuffer.allocate(NONCE_BYTES + ciphertext.length).put(nonce).put(ciphertext).array();
      return String.join(
          ":", VERSION, keyId.orElseThrow(), Base64.getEncoder().encodeToString(payload));
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to encrypt a credential value", e);
    }
  }

  /**
   * @throws BaseException if the server has no key, has another key, or the value does not decrypt
   */
  @Override
  public String load(String storedValue, SecretContext context) {
    String[] parts = storedValue.split(":", 3);
    if (parts.length != 3 || !parts[0].equals(VERSION)) {
      throw cannotLoad(context, "it is not in a known format");
    }
    if (key.isEmpty()) {
      throw cannotLoad(context, "the server has no " + Property.CREDENTIAL_ENCRYPTION_KEY.getKey());
    }
    if (!parts[1].equals(keyId.orElseThrow())) {
      throw cannotLoad(
          context,
          "it was encrypted with key " + parts[1] + ", but the server has key " + keyId.get());
    }
    try {
      byte[] payload = Base64.getDecoder().decode(parts[2]);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(
          Cipher.DECRYPT_MODE, key.get(), new GCMParameterSpec(TAG_BITS, payload, 0, NONCE_BYTES));
      cipher.updateAAD(associatedData(context));
      return new String(
          cipher.doFinal(payload, NONCE_BYTES, payload.length - NONCE_BYTES),
          StandardCharsets.UTF_8);
    } catch (GeneralSecurityException | IllegalArgumentException e) {
      // The cause names the failure, such as a tag mismatch or a malformed payload; it holds no
      // part of the value.
      throw cannotLoad(context, "it does not decrypt with the server's key", e);
    }
  }

  private static BaseException cannotLoad(SecretContext context, String reason) {
    return cannotLoad(context, reason, /* cause= */ null);
  }

  private static BaseException cannotLoad(SecretContext context, String reason, Throwable cause) {
    return new BaseException(
        ErrorCode.INTERNAL,
        "Cannot read the stored value of " + describe(context) + ": " + reason + ".",
        cause);
  }
}
