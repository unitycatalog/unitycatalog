package io.unitycatalog.hadoop.internal.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    return hashCanonical(canonicalBytes(map));
  }

  private static byte[] canonicalBytes(Map<String, String> map) {
    List<Entry<String, String>> entries = new ArrayList<>(map.entrySet());
    entries.sort(Entry.comparingByKey());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Entry<String, String> entry : entries) {
      writeUtf8(out, Objects.requireNonNull(entry.getKey(), "map key cannot be null"));
      writeUtf8(out, Objects.requireNonNull(entry.getValue(), "map value cannot be null"));
    }
    return out.toByteArray();
  }

  private static void writeUtf8(ByteArrayOutputStream out, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeInt(out, bytes.length);
    try {
      out.write(bytes);
    } catch (IOException e) {
      throw new IllegalStateException("failed to write canonical map bytes", e);
    }
  }

  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static String hashCanonical(byte[] canonical) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return toHex(digest.digest(canonical));
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
