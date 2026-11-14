package io.unitycatalog.server.teststorage;

import java.util.Arrays;
import java.util.Optional;

/**
 * The object storage the {@code s3://} paths of the test suite run on, selected with {@value #ENV}
 * (unset means {@link #EMULATED}). This enum is the list; the sbt aliases and {@code AGENTS.md}
 * name its constants too.
 */
public enum SupportedStorage {
  /** Fake file systems and clients that map {@code s3://} paths to local files. */
  EMULATED(null),
  // bitnamilegacy/minio:latest
  MINIO(
      "bitnamilegacy/minio@sha256:451fe6858cb770cc9d0e77ba811ce287420f781c7c1b806a386f6896471a349c"),
  // ubuntu:24.04, which CephStorage builds into a local Ceph 19.2.3 image; used without STS, the
  // server vending no keys (server.s3-compatible-storage.static-key-vending=endpoint-only)
  CEPH("ubuntu@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852"),
  // The same storage, with the server vending the key pair
  // (server.s3-compatible-storage.static-key-vending=static-key)
  CEPH_STATIC_KEY("ubuntu@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852"),
  // chrislusf/seaweedfs:latest (SeaweedFS 4.50)
  SEAWEEDFS(
      "chrislusf/seaweedfs@sha256:c2658820e763294a9e9de7b36c81e59b879e3459148351aee4e31a126ca64860"),
  // rustfs/rustfs:latest (RustFS 1.0.4)
  RUSTFS("rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2"),
  // dxflrs/garage:v2.1.0, which has no STS; the server vends no keys
  // (server.s3-compatible-storage.static-key-vending=endpoint-only)
  GARAGE("dxflrs/garage@sha256:4c9b34c113e61358466e83fd6e7d66e6d18657ede14b776eb78a93ee8da7cf6a"),
  // The same storage, with the server vending the key pair
  // (server.s3-compatible-storage.static-key-vending=static-key)
  GARAGE_STATIC_KEY(
      "dxflrs/garage@sha256:4c9b34c113e61358466e83fd6e7d66e6d18657ede14b776eb78a93ee8da7cf6a");

  public static final String ENV = "UC_TEST_STORAGE";

  private final String image;

  SupportedStorage(String image) {
    this.image = image;
  }

  public static SupportedStorage selected() {
    return parse(System.getenv(ENV));
  }

  private static SupportedStorage parse(String value) {
    if (value == null) {
      return EMULATED;
    }
    return Arrays.stream(values())
        .filter(storage -> storage.name().equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    ENV + "=" + value + " is not one of " + Arrays.toString(values())));
  }

  /** The container image for storage that runs in Docker; empty for {@link #EMULATED}. */
  public Optional<String> image() {
    return Optional.ofNullable(image);
  }
}
