package io.unitycatalog.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Shared fake-S3 logic for the credential-vending integration tests: the object-key-to-local-file
 * mapping, the vended-credential check, and the class name of the mock S3 client factory wired into
 * Iceberg's {@code S3FileIO}. It carries no AWS or Iceberg imports so it stays usable from sources
 * compiled for every Spark version, unlike the AWS-dependent {@code MockS3Client} (Spark 4.0/4.1
 * shims only).
 *
 * <p>Locations follow the same convention as {@link CredentialTestFileSystem}: {@code
 * s3://test-bucket0/<absolute-path>} stands for {@code /<absolute-path>}, so a fake bucket is a
 * label over the local filesystem and the server-side mapping ({@code LocalMappingFileOperations})
 * and client-side mock agree by construction.
 */
public final class FakeS3 {

  /** The bucket configured (index 0) in the test server properties, mapped to a local directory. */
  public static final String BUCKET = "test-bucket0";

  public static final String ACCESS_KEY_ID = "accessKey0";
  public static final String SECRET_ACCESS_KEY = "secretKey0";
  public static final String SESSION_TOKEN = "sessionToken0";

  /**
   * The S3 credentials the server vends for {@link #BUCKET} (configured as static keys in the test
   * server properties), keyed by the Iceberg {@code S3FileIO} property names, for exact-match
   * validation of the vended config on the server side (the client-side {@code MockS3Client}
   * validates the same values via {@link #requireVendedCredentials}).
   */
  public static final Map<String, String> EXPECTED_VENDED_CREDENTIALS =
      Map.of(
          "s3.access-key-id", ACCESS_KEY_ID,
          "s3.secret-access-key", SECRET_ACCESS_KEY,
          "s3.session-token", SESSION_TOKEN);

  /**
   * The mock S3 client factory for Iceberg's {@code S3FileIO}; a string because it is shims-only.
   */
  public static final String MOCK_S3_CLIENT_FACTORY = "io.unitycatalog.spark.MockS3ClientFactory";

  private FakeS3() {}

  /**
   * Maps the S3 object key of an {@code s3://<bucket>/<key>} location to the absolute local file it
   * stands in for, mirroring {@link CredentialTestFileSystem}: the bucket is a label, the key is an
   * absolute path.
   */
  public static Path toLocalPath(String key) {
    return Paths.get("/" + key);
  }

  /**
   * Fails loudly unless the credentials handed to the fake are exactly the ones UC vends, so a test
   * proves the vended credentials actually reached the storage client rather than the ambient
   * default chain. Names only the field that differs; it does not echo the values.
   */
  public static void requireVendedCredentials(
      String accessKeyId, String secretKey, String sessionToken) {
    require("s3.access-key-id", accessKeyId, ACCESS_KEY_ID);
    require("s3.secret-access-key", secretKey, SECRET_ACCESS_KEY);
    require("s3.session-token", sessionToken, SESSION_TOKEN);
  }

  private static void require(String field, String actual, String expected) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          "fake S3 was handed a " + field + " that is not the credential UC vended");
    }
  }

  /**
   * Applies an HTTP-style {@code bytes=start-end} range (open-ended end allowed) to {@code data}.
   */
  public static byte[] applyRange(byte[] data, String range) {
    if (range == null || !range.startsWith("bytes=")) {
      return data;
    }
    String[] parts = range.substring("bytes=".length()).split("-", 2);
    int start = parts[0].isEmpty() ? 0 : Integer.parseInt(parts[0]);
    int end =
        parts.length > 1 && !parts[1].isEmpty() ? Integer.parseInt(parts[1]) : data.length - 1;
    end = Math.min(end, data.length - 1);
    if (start > end) {
      return new byte[0];
    }
    byte[] slice = new byte[end - start + 1];
    System.arraycopy(data, start, slice, 0, slice.length);
    return slice;
  }
}
