package io.unitycatalog.hadoop.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The connector's single source of truth for cloud storage types and their schemes. */
public enum CloudType {
  S3("s3", "s3a"),
  GCS("gs"),
  ABFS("abfs", "abfss");

  private final Set<String> schemes;

  private static final Map<String, CloudType> BY_SCHEME = new HashMap<>();

  static {
    for (CloudType type : values()) {
      for (String scheme : type.schemes) {
        BY_SCHEME.put(scheme, type);
      }
    }
  }

  CloudType(String... schemes) {
    this.schemes = Set.of(schemes);
  }

  /** Resolves a scheme to its underlying {@link CloudType}. */
  public static Optional<CloudType> fromScheme(String scheme) {
    return Optional.ofNullable(BY_SCHEME.get(scheme));
  }
}
