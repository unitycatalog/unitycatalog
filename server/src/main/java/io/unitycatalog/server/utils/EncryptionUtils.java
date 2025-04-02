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
 * Utility class for handling encryption and decryption operations.
 * 
 * <p>This class provides methods to securely encrypt and decrypt sensitive data
 * such as cloud credentials. It uses AES-GCM (Galois/Counter Mode) which provides
 * both confidentiality and authentication.
 * 
 * <p>The encryption key can be provided through:
 * <ol>
 *   <li>System property "encryption.key"</li>
 *   <li>Environment variable "encryption.key"</li>
 * </ol>
 * 
 * <p>If no key is provided, a random key will be generated on startup. Note that this 
 * approach is not suitable for production as it will generate a new key each time 
 * the application starts, making previously encrypted data unreadable. In a production 
 * environment, always provide a consistent encryption key.
 * 
 * <p>The generated key is a 256-bit AES key encoded as a Base64 string.
 * 
 * <p>Example usage:
 * <pre>
 * // Encrypt sensitive data
 * String sensitiveData = "secret-password";
 * String encrypted = EncryptionUtils.encrypt(sensitiveData);
 * 
 * // Decrypt when needed
 * String decrypted = EncryptionUtils.decrypt(encrypted);
 * </pre>
 */
public class EncryptionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionUtils.class);
  
  /** The encryption algorithm used (AES in GCM mode with no padding) */
  private static final String ALGORITHM = "AES/GCM/NoPadding";
  
  /** Length of the Initialization Vector (IV) in bytes */
  private static final int GCM_IV_LENGTH = 12;
  
  /** Length of the GCM authentication tag in bits */
  private static final int GCM_TAG_LENGTH = 128;
  
  /** Property name for the encryption key */
  private static final String KEY_PROPERTY = "encryption.key";
  
  /** The secret key used for encryption and decryption */
  private static SecretKey secretKey;

  /**
   * Static initializer that sets up the encryption key on class loading.
   * Tries to get a key from properties or environment variables, 
   * falling back to generating a new key if none is found.
   */
  static {
    initializeSecretKey();
  }

  /**
   * Initializes the secret key used for encryption/decryption operations.
   * 
   * <p>Checks for an existing key in the system properties or environment variables.
   * If no key is found or the provided key is invalid, generates a new key.
   */
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

  /**
   * Generates a new random AES-256 encryption key.
   * 
   * <p>This method is called when no valid key is provided through properties
   * or environment variables. The generated key is logged as a warning since
   * using a randomly generated key in production is not recommended.
   * 
   * @throws BaseException if key generation fails
   */
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
   * <p>The encryption process:
   * <ol>
   *   <li>Generates a random initialization vector (IV)</li>
   *   <li>Encrypts the data using AES-GCM with the IV and secret key</li>
   *   <li>Combines the IV and encrypted data</li>
   *   <li>Encodes the result as a Base64 string</li>
   * </ol>
   *
   * @param plaintext The text to encrypt
   * @return Base64-encoded encrypted string including IV
   * @throws BaseException if encryption fails for any reason
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
   * <p>The decryption process:
   * <ol>
   *   <li>Decodes the Base64 string</li>
   *   <li>Extracts the IV from the beginning of the decoded data</li>
   *   <li>Extracts the ciphertext from the remainder of the decoded data</li>
   *   <li>Decrypts using AES-GCM with the extracted IV and the secret key</li>
   * </ol>
   *
   * @param encryptedData Base64-encoded encrypted string including IV
   * @return The decrypted plaintext
   * @throws BaseException if decryption fails for any reason (including authentication failure)
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
