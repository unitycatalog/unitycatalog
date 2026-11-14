package io.unitycatalog.server.teststorage;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * A single-node Ceph cluster in a container, one per JVM: a monitor, an in-memory OSD and the RADOS
 * Gateway (RGW). The tests use it as storage without STS: Unity Catalog's credential holds a plain
 * key pair of an RGW user, and the server's {@code server.s3-compatible-storage.static-key-vending}
 * decides whether clients receive it.
 *
 * <p>RGW before 19.2.5, 20.2.2 and 21.0.1 answers a HEAD or GET of a missing key with 403 when the
 * caller's ListBucket grant is conditioned on {@code s3:prefix}, as in the session policies Unity
 * Catalog vends, where AWS S3 answers 404 for a key under the granted prefix
 * (https://tracker.ceph.com/issues/74398). Hadoop S3A and Iceberg probe missing keys and stop on a
 * 403, so they cannot use scoped sessions on those versions. TODO: once Ubuntu 24.04 ships 19.2.5
 * or later, move to it and run Ceph with STS.
 *
 * <p>Ceph publishes current images only outside Docker Hub, so the image is built here from Ubuntu
 * 24.04's Ceph 19.2.3 packages and kept in the local Docker image cache.
 */
final class CephStorage implements TestStorage {

  private static final String CEPH_VERSION = "19.2.3-0ubuntu0.24.04.3";
  private static final int S3_PORT = 7480;
  private static final String USER = "uc-test-user";
  private static final String USER_SECRET = "uc-test-user-value";
  private static final String HOST_FIPS_FLAG = "/proc/sys/crypto/fips_enabled";
  private static final Duration READY_TIMEOUT = Duration.ofMinutes(5);

  /** Starts the cluster and keeps the container running. */
  private static final String START_SCRIPT =
      String.join(
          "\n",
          "#!/bin/bash",
          "set -e",
          "fsid=$(uuidgen)",
          "mkdir -p /etc/ceph /var/lib/ceph/mon/ceph-a /var/lib/ceph/osd/ceph-0 /var/run/ceph",
          "cat > /etc/ceph/ceph.conf <<CONF",
          "[global]",
          "fsid = $fsid",
          "mon_host = 127.0.0.1",
          "mon_initial_members = a",
          // Test-only: no cephx between the daemons of the single container.
          "auth_cluster_required = none",
          "auth_service_required = none",
          "auth_client_required = none",
          "osd_pool_default_size = 1",
          "osd_pool_default_min_size = 1",
          "mon_allow_pool_size_one = true",
          "mon_warn_on_pool_no_redundancy = false",
          "osd_objectstore = memstore",
          "memstore_device_bytes = 4294967296",
          "ms_bind_ipv6 = false",
          "osd_pool_default_pg_autoscale_mode = off",
          "[client.rgw.a]",
          "rgw_frontends = beast endpoint=0.0.0.0:" + S3_PORT,
          "CONF",
          "monmaptool --create --add a 127.0.0.1 --fsid $fsid /tmp/monmap >/dev/null",
          "ceph-mon --mkfs -i a --monmap /tmp/monmap",
          "ceph-mon -i a",
          "ceph osd create >/dev/null",
          "ceph-osd -i 0 --mkfs",
          "ceph-osd -i 0",
          "until ceph osd stat | grep -q '1 up'; do sleep 1; done",
          "radosgw -n client.rgw.a",
          "until curl -s -o /dev/null http://127.0.0.1:" + S3_PORT + "; do sleep 1; done",
          "echo 'CEPH READY'",
          "exec tail -f /dev/null",
          "");

  private static GenericContainer<?> container;

  private final S3CompatibleStaticKeyVending staticKeyVending;

  private CephStorage(S3CompatibleStaticKeyVending staticKeyVending) {
    this.staticKeyVending = staticKeyVending;
  }

  /**
   * @param staticKeyVending the mode the tests run the server in
   * @return the Ceph storage, its container built and set up on first use
   */
  static synchronized CephStorage get(S3CompatibleStaticKeyVending staticKeyVending) {
    if (container == null) {
      GenericContainer<?> started =
          fipsOff(new GenericContainer<>(DockerImageName.parse(ensureImage())))
              .withExposedPorts(S3_PORT)
              .waitingFor(
                  Wait.forLogMessage(".*CEPH READY.*", 1).withStartupTimeout(READY_TIMEOUT));
      try {
        started.start();
        setUpUser(started);
      } catch (RuntimeException e) {
        started.stop();
        throw e;
      }
      container = started;
    }
    return new CephStorage(staticKeyVending);
  }

  /**
   * Builds the Ceph image from the pinned Ubuntu base unless the local image cache has it. The tag
   * covers the package version and the start script, so a change to either builds a new image. It
   * is built by running the install in a container and committing it, not with a Dockerfile: a
   * build cannot hide a FIPS-mode host from the package scripts.
   *
   * @return the image name
   */
  private static String ensureImage() {
    String image = "uc-test-ceph:" + CEPH_VERSION.replace('~', '-') + "-" + scriptHash();
    DockerClient docker = DockerClientFactory.instance().client();
    try {
      docker.inspectImageCmd(image).exec();
      return image;
    } catch (NotFoundException e) {
      // Built below.
    }
    String packages =
        String.join(
            " ",
            "ceph-mon=" + CEPH_VERSION,
            "ceph-osd=" + CEPH_VERSION,
            "radosgw=" + CEPH_VERSION,
            "ceph-common=" + CEPH_VERSION,
            "uuid-runtime",
            "curl");
    try (GenericContainer<?> builder =
        fipsOff(
            new GenericContainer<>(
                    DockerImageName.parse(SupportedStorage.CEPH.image().orElseThrow()))
                .withCommand("sleep", "infinity")
                .withCopyToContainer(Transferable.of(START_SCRIPT, 0755), "/start.sh"))) {
      builder.start();
      exec(
          builder,
          "sh",
          "-c",
          "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y"
              + " --no-install-recommends "
              + packages
              + " && rm -rf /var/lib/apt/lists/*");
      String[] nameAndTag = image.split(":", 2);
      docker
          .commitCmd(builder.getContainerId())
          .withRepository(nameAndTag[0])
          .withTag(nameAndTag[1])
          .withCmd("/start.sh")
          // The image would carry the builder's session label, so Testcontainers would remove it
          // when the JVM exits.
          .withLabels(Map.of(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL, "uc-test-ceph"))
          .exec();
    }
    return image;
  }

  private static String scriptHash() {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(START_SCRIPT.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 6);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * On a host in FIPS mode, hides the mode from the container: Ceph's daemons and the package
   * scripts use digests that OpenSSL refuses in FIPS mode, and a container sees the host's flag.
   */
  private static GenericContainer<?> fipsOff(GenericContainer<?> container) {
    if (hostInFipsMode()) {
      container.withFileSystemBind(fipsOffFlag().toString(), HOST_FIPS_FLAG, BindMode.READ_ONLY);
    }
    return container;
  }

  private static boolean hostInFipsMode() {
    try {
      Path flag = Path.of(HOST_FIPS_FLAG);
      return Files.exists(flag) && Files.readString(flag).trim().equals("1");
    } catch (IOException e) {
      return false;
    }
  }

  private static Path fipsOffFlag() {
    try {
      Path flag = Files.createTempFile("uc-test-fips-off", "");
      Files.writeString(flag, "0\n");
      flag.toFile().deleteOnExit();
      return flag;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The RGW user whose key Unity Catalog holds. */
  private static void setUpUser(GenericContainer<?> ceph) {
    exec(
        ceph,
        "radosgw-admin",
        "user",
        "create",
        "--uid=" + USER,
        "--display-name=" + USER,
        "--access-key=" + USER,
        "--secret=" + USER_SECRET);
  }

  private static void exec(GenericContainer<?> target, String... command) {
    ExecResult result;
    try {
      result = target.execInContainer(StandardCharsets.UTF_8, command);
    } catch (IOException e) {
      throw new IllegalStateException("Could not run " + String.join(" ", command), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted running " + String.join(" ", command), e);
    }
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          String.join(" ", command) + " failed: " + result.getStdout() + result.getStderr());
    }
  }

  @Override
  public String s3Endpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT);
  }

  @Override
  public String accessKeyId() {
    return USER;
  }

  @Override
  public String secretAccessKey() {
    return USER_SECRET;
  }

  @Override
  public Optional<String> roleArn() {
    return Optional.empty();
  }

  @Override
  public S3CompatibleStaticKeyVending staticKeyVending() {
    return staticKeyVending;
  }

  @Override
  public synchronized void createBucket(String bucket) {
    S3Buckets.create(s3Endpoint(), USER, USER_SECRET, bucket);
  }
}
