package io.unitycatalog.server.teststorage;

import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Garage in a container, one per JVM: a single node holding every replica. Garage has no STS, so
 * Unity Catalog's credential holds a plain key pair, and the server's {@code
 * server.s3-compatible-storage.static-key-vending} decides whether clients receive it. Garage only
 * accepts requests signed for its configured region; it is the region Unity Catalog falls back to,
 * since Garage does not report one.
 */
final class GarageStorage implements TestStorage {

  private static final int S3_PORT = 3900;
  // Garage key IDs are "GK" and 24 hex digits; secrets are 64 hex digits.
  private static final String KEY_ID = "GK0123456789abcdef01234567";
  private static final String KEY_SECRET =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final String CONFIG =
      String.join(
          "\n",
          "metadata_dir = \"/var/lib/garage/meta\"",
          "data_dir = \"/var/lib/garage/data\"",
          "db_engine = \"sqlite\"",
          "replication_factor = 1",
          "rpc_bind_addr = \"[::]:3901\"",
          "rpc_public_addr = \"127.0.0.1:3901\"",
          "rpc_secret = \"1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec\"",
          "[s3_api]",
          "s3_region = \"us-east-1\"",
          "api_bind_addr = \"[::]:" + S3_PORT + "\"",
          "root_domain = \".s3.garage.localhost\"",
          "");

  private static GenericContainer<?> container;

  private final S3CompatibleStaticKeyVending staticKeyVending;

  private GarageStorage(S3CompatibleStaticKeyVending staticKeyVending) {
    this.staticKeyVending = staticKeyVending;
  }

  /**
   * @param staticKeyVending the mode the tests run the server in
   * @return the Garage storage, its container started and set up on first use
   */
  static synchronized GarageStorage get(S3CompatibleStaticKeyVending staticKeyVending) {
    if (container == null) {
      // Garage rejects the streaming payload signature that AWS SDK clients, such as Iceberg's
      // S3FileIO, send with a checksum by default; deployments set the same through the
      // AWS_REQUEST_CHECKSUM_CALCULATION environment variable.
      GenericContainer<?> started =
          new GenericContainer<>(
                  DockerImageName.parse(SupportedStorage.GARAGE.image().orElseThrow()))
              .withCopyToContainer(Transferable.of(CONFIG), "/etc/garage.toml")
              .withExposedPorts(S3_PORT)
              // The image has no shell for an in-container port check, and Docker accepts the
              // mapped port before Garage listens on it.
              .waitingFor(Wait.forLogMessage(".*S3 API server listening.*", 1));
      try {
        started.start();
        System.setProperty("aws.requestChecksumCalculation", "when_required");
        System.setProperty("aws.responseChecksumValidation", "when_required");
        setUp(started);
      } catch (RuntimeException e) {
        System.clearProperty("aws.requestChecksumCalculation");
        System.clearProperty("aws.responseChecksumValidation");
        started.stop();
        throw e;
      }
      container = started;
    }
    return new GarageStorage(staticKeyVending);
  }

  /** Gives the node all the capacity, and imports the key, allowed to create buckets. */
  private static void setUp(GenericContainer<?> garage) {
    String nodeId = garage("node", garage, "id", "-q").split("@", 2)[0].trim();
    garage("layout", garage, "assign", "-z", "dc1", "-c", "1G", nodeId);
    garage("layout", garage, "apply", "--version", "1");
    garage("key", garage, "import", "--yes", "-n", "uc-test", KEY_ID, KEY_SECRET);
    garage("key", garage, "allow", "--create-bucket", KEY_ID);
  }

  /** Runs a {@code garage} command in the container and returns its output. */
  private static String garage(String command, GenericContainer<?> garage, String... args) {
    String[] full = new String[args.length + 2];
    full[0] = "/garage";
    full[1] = command;
    System.arraycopy(args, 0, full, 2, args.length);
    ExecResult result;
    try {
      result = garage.execInContainer(StandardCharsets.UTF_8, full);
    } catch (IOException e) {
      throw new IllegalStateException("Could not run " + String.join(" ", full), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted running " + String.join(" ", full), e);
    }
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          String.join(" ", full) + " failed: " + result.getStdout() + result.getStderr());
    }
    return result.getStdout();
  }

  @Override
  public String s3Endpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT);
  }

  @Override
  public String accessKeyId() {
    return KEY_ID;
  }

  @Override
  public String secretAccessKey() {
    return KEY_SECRET;
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
    S3Buckets.create(s3Endpoint(), KEY_ID, KEY_SECRET, bucket);
  }
}
