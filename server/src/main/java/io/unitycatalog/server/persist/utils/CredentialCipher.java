package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import jakarta.persistence.AttributeConverter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encrypts the stored credential column with AES-256-GCM under the operator's key. An encrypted
 * value is {@code enc:v1:} followed by the base64 of a 12-byte IV and the ciphertext. Values
 * without that prefix, written before a key was set, are read as plaintext. Without a key, values
 * are stored as plaintext.
 */
public class CredentialCipher implements AttributeConverter<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialCipher.class);
  static final String PREFIX = "enc:v1:";
  private static final int IV_BYTES = 12;
  private static final int TAG_BITS = 128;
  private static final SecureRandom RANDOM = new SecureRandom();

  // Null when no key is set.
  private final SecretKey key;

  CredentialCipher(SecretKey key) {
    this.key = key;
  }

  /**
   * Builds the cipher from {@code server.credential-encryption-key} (a base64 32-byte key, for
   * example from the SERVER_CREDENTIAL_ENCRYPTION_KEY environment variable) or {@code
   * server.credential-encryption-key-file} (a file holding that key). Setting both is an error.
   */
  public static CredentialCipher from(ServerProperties serverProperties) {
    String encodedKey = serverProperties.get(Property.CREDENTIAL_ENCRYPTION_KEY);
    String keyFile = serverProperties.get(Property.CREDENTIAL_ENCRYPTION_KEY_FILE);
    if (encodedKey != null && keyFile != null) {
      throw new IllegalStateException(
          String.format(
              "Set only one of %s and %s",
              Property.CREDENTIAL_ENCRYPTION_KEY.getKey(),
              Property.CREDENTIAL_ENCRYPTION_KEY_FILE.getKey()));
    }
    if (keyFile != null) {
      try {
        encodedKey = Files.readString(Path.of(keyFile)).trim();
      } catch (IOException e) {
        throw new IllegalStateException(
            "Cannot read the credential encryption key file " + Path.of(keyFile).toAbsolutePath(),
            e);
      }
    }
    if (encodedKey == null) {
      LOGGER.warn(
          "Neither {} nor {} is set, so storage credentials are stored unencrypted.",
          Property.CREDENTIAL_ENCRYPTION_KEY.getKey(),
          Property.CREDENTIAL_ENCRYPTION_KEY_FILE.getKey());
      return new CredentialCipher(null);
    }
    return new CredentialCipher(new SecretKeySpec(decodeKey(encodedKey), "AES"));
  }

  private static byte[] decodeKey(String encodedKey) {
    byte[] keyBytes;
    try {
      keyBytes = Base64.getDecoder().decode(encodedKey.trim());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("The credential encryption key is not valid base64", e);
    }
    if (keyBytes.length != 32) {
      throw new IllegalStateException(
          "The credential encryption key must be 32 bytes in base64, for example the output of"
              + " `openssl rand -base64 32`");
    }
    return keyBytes;
  }

  @Override
  public String convertToDatabaseColumn(String plaintext) {
    if (key == null || plaintext == null) {
      return plaintext;
    }
    try {
      byte[] iv = new byte[IV_BYTES];
      RANDOM.nextBytes(iv);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
      byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
      byte[] stored =
          ByteBuffer.allocate(IV_BYTES + ciphertext.length).put(iv).put(ciphertext).array();
      return PREFIX + Base64.getEncoder().encodeToString(stored);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Cannot encrypt the credential", e);
    }
  }

  @Override
  public String convertToEntityAttribute(String stored) {
    if (stored == null || !stored.startsWith(PREFIX)) {
      return stored;
    }
    if (key == null) {
      throw new IllegalStateException(
          "A stored credential is encrypted, but no credential encryption key is set");
    }
    try {
      byte[] decoded = Base64.getDecoder().decode(stored.substring(PREFIX.length()));
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, decoded, 0, IV_BYTES));
      byte[] plaintext = cipher.doFinal(decoded, IV_BYTES, decoded.length - IV_BYTES);
      return new String(plaintext, StandardCharsets.UTF_8);
    } catch (GeneralSecurityException | IllegalArgumentException e) {
      throw new IllegalStateException(
          "Cannot decrypt a stored credential; the credential encryption key may be wrong", e);
    }
  }
}
