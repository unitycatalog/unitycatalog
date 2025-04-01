package io.unitycatalog.server.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class EncryptionUtilsTest {

  @Test
  public void testEncryptionAndDecryption() {
    // Test with simple string
    String original = "test-string";
    String encrypted = EncryptionUtils.encrypt(original);

    // Ensure encrypted is different from original
    assertNotEquals(original, encrypted);

    // Decrypt and verify it matches original
    String decrypted = EncryptionUtils.decrypt(encrypted);
    assertEquals(original, decrypted);

    // Test with JSON-like string
    String jsonData = "{\"roleArn\":\"arn:aws:iam::123456789012:role/test-role\"}";
    String encryptedJson = EncryptionUtils.encrypt(jsonData);
    String decryptedJson = EncryptionUtils.decrypt(encryptedJson);
    assertEquals(jsonData, decryptedJson);

    // Ensure different encryptions of the same text produce different results (due to IV)
    String encrypted2 = EncryptionUtils.encrypt(original);
    assertNotEquals(encrypted, encrypted2);
  }

  @Test
  public void testEncryptionWithEmptyString() {
    String original = "";
    String encrypted = EncryptionUtils.encrypt(original);
    String decrypted = EncryptionUtils.decrypt(encrypted);
    assertEquals(original, decrypted);
  }

  @Test
  public void testEncryptionWithLongString() {
    StringBuilder longString = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longString.append("abcdefghijklmnopqrstuvwxyz");
    }
    String original = longString.toString();
    String encrypted = EncryptionUtils.encrypt(original);
    String decrypted = EncryptionUtils.decrypt(encrypted);
    assertEquals(original, decrypted);
  }
}
