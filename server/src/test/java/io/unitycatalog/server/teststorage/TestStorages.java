package io.unitycatalog.server.teststorage;

import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import java.util.Optional;

/** Hands out the {@link TestStorage} selected by {@value SupportedStorage#ENV}. */
public final class TestStorages {

  // The first failure to start the selected storage, rethrown on later calls so that the tests
  // after it fail at once rather than each start it again.
  private static RuntimeException startFailure;

  private TestStorages() {}

  /**
   * @return the selected S3-compatible storage, started on first use and shared by the JVM; empty
   *     for {@link SupportedStorage#EMULATED}
   * @throws IllegalStateException if it failed to start earlier in this JVM
   */
  public static synchronized Optional<TestStorage> selected() {
    if (startFailure != null) {
      throw new IllegalStateException(
          "The selected test storage failed to start earlier", startFailure);
    }
    try {
      return start();
    } catch (RuntimeException e) {
      startFailure = e;
      throw e;
    }
  }

  private static Optional<TestStorage> start() {
    return switch (SupportedStorage.selected()) {
      case EMULATED -> Optional.empty();
      case MINIO -> Optional.of(MinioStorage.get());
      case CEPH -> Optional.of(CephStorage.get(S3CompatibleStaticKeyVending.ENDPOINT_ONLY));
      case CEPH_STATIC_KEY -> Optional.of(CephStorage.get(S3CompatibleStaticKeyVending.STATIC_KEY));
      case RUSTFS -> Optional.of(RustfsStorage.get());
      case SEAWEEDFS -> Optional.of(SeaweedfsStorage.get());
      case GARAGE -> Optional.of(GarageStorage.get(S3CompatibleStaticKeyVending.ENDPOINT_ONLY));
      case GARAGE_STATIC_KEY ->
          Optional.of(GarageStorage.get(S3CompatibleStaticKeyVending.STATIC_KEY));
    };
  }
}
