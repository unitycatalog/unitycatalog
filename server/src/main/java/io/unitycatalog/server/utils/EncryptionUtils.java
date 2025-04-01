package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling encryption and decryption operations. Uses AES-GCM for secure
 * encryption with authentication.
 */
public class EncryptionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionUtils.class);
  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_IV_LENGTH = 12;
  private static final int GCM_TAG_LENGTH = 128; // in bits
  private static final String KEY_PROPERTY = "encryption.key";
  private static SecretKey secretKey;

  static {
    initializeSecretKey();
  }

  private static void initializeSecretKey() {
    // Try to get key from properties or environment
    String base64Key = System.getProperty(KEY_PROPERTY);
    if (base64Key == null) {
      base64Key = System.getenv(KEY_PROPERTY);
    }

    if (base64Key != null) {
      try {
        byte[] decodedKey = Base64.getDecoder().decode(base64Key);
        secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
        LOGGER.info("Using provided encryption key");
      } catch (IllegalArgumentException e) {
        LOGGER.error("Invalid encryption key format, generating a new one", e);
        generateNewKey();
      }
    } else {
      generateNewKey();
    }
  }

  private static void generateNewKey() {
    try {
      KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
      keyGenerator.init(256, new SecureRandom());
      secretKey = keyGenerator.generateKey();
      String encodedKey = Base64.getEncoder().encodeToString(secretKey.getEncoded());
      LOGGER.warn(
          "Generated new encryption key. For production, set this key in environment: {}={}",
          KEY_PROPERTY,
          encodedKey);
    } catch (NoSuchAlgorithmException e) {
      LOGGER.error("Failed to generate encryption key", e);
      throw new BaseException(
          ErrorCode.INTERNAL, "Failed to initialize encryption: " + e.getMessage());
    }
  }

  /**
   * Encrypts a plaintext string using AES-GCM.
   *
   * @param plaintext The text to encrypt
   * @return Base64-encoded encrypted string including IV
   */
  public static String encrypt(String plaintext) {
    try {
      // Generate a random IV
      byte[] iv = new byte[GCM_IV_LENGTH];
      new SecureRandom().nextBytes(iv);

      // Initialize cipher for encryption
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmParameterSpec);

      // Encrypt
      byte[] encryptedData = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

      // Combine IV and encrypted data and encode
      byte[] combined = new byte[iv.length + encryptedData.length];
      System.arraycopy(iv, 0, combined, 0, iv.length);
      System.arraycopy(encryptedData, 0, combined, iv.length, encryptedData.length);

      return Base64.getEncoder().encodeToString(combined);
    } catch (Exception e) {
      LOGGER.error("Encryption failed", e);
      throw new BaseException(ErrorCode.INTERNAL, "Failed to encrypt data: " + e.getMessage());
    }
  }

  /**
   * Decrypts a previously encrypted string using AES-GCM.
   *
   * @param encryptedData Base64-encoded encrypted string including IV
   * @return The decrypted plaintext
   */
  public static String decrypt(String encryptedData) {
    try {
      // Decode the Base64 string
      byte[] combined = Base64.getDecoder().decode(encryptedData);

      // Extract IV and ciphertext
      byte[] iv = new byte[GCM_IV_LENGTH];
      byte[] cipherText = new byte[combined.length - GCM_IV_LENGTH];
      System.arraycopy(combined, 0, iv, 0, iv.length);
      System.arraycopy(combined, iv.length, cipherText, 0, cipherText.length);

      // Initialize cipher for decryption
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);

      // Decrypt
      byte[] decryptedData = cipher.doFinal(cipherText);
      return new String(decryptedData, StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOGGER.error("Decryption failed", e);
      throw new BaseException(ErrorCode.INTERNAL, "Failed to decrypt data: " + e.getMessage());
    }
  }
}
