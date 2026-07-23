package io.unitycatalog.hadoop.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The connector's single source of truth for cloud storage types and their schemes. */
public enum CloudType {
  S3("s3", "s3a"),
  GCS("gs"),
  ABFS("abfs", "abfss");

  private static final Map<String, CloudType> BY_SCHEME = new HashMap<>();

  static {
    for (CloudType type : values()) {
      for (String scheme : type.schemes) {
        BY_SCHEME.put(scheme, type);
      }
    }
  }

  private final List<String> schemes;

  /** The first scheme is the canonical one; the rest are aliases that normalize to it. */
  CloudType(String... schemes) {
    this.schemes = List.of(schemes);
  }

  /** Resolves a scheme to its underlying {@link CloudType}. */
  public static Optional<CloudType> fromScheme(String scheme) {
    return Optional.ofNullable(BY_SCHEME.get(scheme));
  }

  /** The canonical scheme for this cloud; every alias normalizes to it. */
  public String canonicalScheme() {
    return schemes.get(0);
  }
}
