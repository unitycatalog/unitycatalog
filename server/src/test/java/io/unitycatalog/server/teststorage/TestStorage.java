package io.unitycatalog.server.teststorage;

import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import java.util.Optional;

/**
 * S3-compatible storage the tests' {@code s3://} paths live on. Unity Catalog reaches it the way a
 * deployment does: through an {@code s3_compatible} storage credential carrying {@link
 * #s3Endpoint()} and the access key of a storage user, which assumes {@link #roleArn()} with STS
 * where the storage's sessions can serve the tests.
 */
public interface TestStorage {

  /**
   * The S3 API endpoint, for example {@code http://localhost:32768}; it also serves STS on storage
   * with a {@link #roleArn}.
   */
  String s3Endpoint();

  /** The access key ID of the storage user that Unity Catalog's storage credential holds. */
  String accessKeyId();

  /** The secret access key paired with {@link #accessKeyId()}. */
  String secretAccessKey();

  /**
   * @return the role Unity Catalog's credential assumes with STS AssumeRole, so each vend is a
   *     session; empty for storage without STS, where {@link #staticKeyVending()} decides what is
   *     vended
   */
  Optional<String> roleArn();

  /**
   * @return the server's {@code server.s3-compatible-storage.static-key-vending} for this storage.
   *     It only matters without {@link #roleArn()}: {@code endpoint-only} vends no keys, so engines
   *     use {@link #accessKeyId()} themselves; {@code static-key} vends the key pair.
   */
  default S3CompatibleStaticKeyVending staticKeyVending() {
    return S3CompatibleStaticKeyVending.ENDPOINT_ONLY;
  }

  /** Whether clients receive keys from Unity Catalog, rather than bringing their own. */
  default boolean vendsKeys() {
    return roleArn().isPresent() || staticKeyVending() == S3CompatibleStaticKeyVending.STATIC_KEY;
  }

  /**
   * Whether the storage applies the inline session policy of AssumeRole. Unity Catalog narrows each
   * vended session to its path with that policy; on storage that ignores it, or vends no sessions,
   * a vended credential reaches everything the storage user reaches.
   */
  default boolean enforcesSessionPolicies() {
    return roleArn().isPresent();
  }

  /**
   * Creates a bucket if it does not exist yet.
   *
   * @param bucket the bucket name
   */
  void createBucket(String bucket);
}
