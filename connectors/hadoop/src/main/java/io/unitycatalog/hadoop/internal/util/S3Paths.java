package io.unitycatalog.hadoop.internal.util;

import java.util.Optional;
import org.apache.hadoop.util.Preconditions;

/** Buckets of S3 storage paths. */
public final class S3Paths {
  private S3Paths() {}

  /**
   * The bucket of an {@code s3://bucket/path} prefix, read as text: bucket names may hold an
   * underscore, which {@code URI.getHost} rejects, and paths may hold characters a URI rejects.
   *
   * @param prefix the storage path
   * @return the bucket
   * @throws IllegalArgumentException if {@code prefix} is not an S3 path with a bucket
   */
  public static String bucketOf(String prefix) {
    int start = prefix.indexOf("://");
    Preconditions.checkArgument(start > 0, "Not an S3 path: %s", prefix);
    int end = prefix.indexOf('/', start + 3);
    String bucket = end < 0 ? prefix.substring(start + 3) : prefix.substring(start + 3, end);
    Preconditions.checkArgument(!bucket.isEmpty(), "No bucket in S3 path: %s", prefix);
    return bucket;
  }

  /**
   * @param prefix a storage path, or null
   * @return the bucket of {@code prefix}, or empty if it names none
   */
  public static Optional<String> bucketIfAny(String prefix) {
    if (prefix == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(bucketOf(prefix));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
