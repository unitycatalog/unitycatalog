package io.unitycatalog.hadoop.internal.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Generates a stable string id for a {@code Map<String, String>} so it can be used as a map key
 * instead of the map itself.
 */
public final class MapIdGenerator {
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private MapIdGenerator() {}

  /**
   * Returns a stable id for {@code map}. Maps with the same key-value pairs produce the same id
   * regardless of iteration order; maps with different pairs produce different ids.
   */
  public static String generateId(Map<String, String> map) {
    Objects.requireNonNull(map, "map cannot be null");
    List<Entry<String, String>> entries = new ArrayList<>(map.entrySet());
    entries.sort(Entry.comparingByKey());

    MessageDigest digest = sha256();
    for (Entry<String, String> entry : entries) {
      update(digest, Objects.requireNonNull(entry.getKey(), "map key cannot be null"));
      update(digest, Objects.requireNonNull(entry.getValue(), "map value cannot be null"));
    }
    return toHex(digest.digest());
  }

  /** Feeds a length-prefixed UTF-8 encoding of {@code value} into the digest. */
  private static void update(MessageDigest digest, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    int length = bytes.length;
    digest.update((byte) (length >>> 24));
    digest.update((byte) (length >>> 16));
    digest.update((byte) (length >>> 8));
    digest.update((byte) length);
    digest.update(bytes);
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String toHex(byte[] bytes) {
    char[] hex = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int value = bytes[i] & 0xFF;
      hex[i * 2] = HEX[value >>> 4];
      hex[i * 2 + 1] = HEX[value & 0x0F];
    }
    return new String(hex);
  }
}
