package io.unitycatalog.server.teststorage;

import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * RustFS in a container, one per JVM. The root user only sets it up: Unity Catalog's credential
 * holds a separate storage user whose policy allows all S3 actions, and the session policy Unity
 * Catalog sends with AssumeRole narrows each vended credential to its path. RustFS answers a HEAD
 * of a missing key under the granted prefix with 404, as AWS S3 does.
 *
 * <p>The RustFS image ships no admin client, so a container of the MinIO image runs {@code mc}
 * against RustFS's MinIO-compatible admin API.
 */
final class RustfsStorage implements TestStorage {

  private static final int S3_PORT = 9000;
  private static final String HOST = "rustfs";
  private static final String ROOT_USER = "uc-test-admin";
  private static final String ROOT_PASSWORD = "uc-test-admin-value";
  private static final String USER = "uc-test-user";
  private static final String USER_SECRET = "uc-test-user-value";
  private static final String POLICY = "uc-test-rw";
  private static final String ALIAS = "local";

  private static RustfsStorage instance;

  private final GenericContainer<?> container;
  private final GenericContainer<?> admin;

  private RustfsStorage(GenericContainer<?> container, GenericContainer<?> admin) {
    this.container = container;
    this.admin = admin;
  }

  /** The RustFS container, started and set up on first use. */
  static synchronized RustfsStorage get() {
    if (instance == null) {
      Network network = Network.newNetwork();
      GenericContainer<?> container =
          new GenericContainer<>(
                  DockerImageName.parse(SupportedStorage.RUSTFS.image().orElseThrow()))
              .withNetwork(network)
              .withNetworkAliases(HOST)
              .withExposedPorts(S3_PORT)
              .withEnv("RUSTFS_ACCESS_KEY", ROOT_USER)
              .withEnv("RUSTFS_SECRET_KEY", ROOT_PASSWORD)
              .waitingFor(Wait.forListeningPort());
      GenericContainer<?> admin =
          new GenericContainer<>(
                  DockerImageName.parse(SupportedStorage.MINIO.image().orElseThrow()))
              .withNetwork(network)
              .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sleep"))
              .withCommand("infinity");
      RustfsStorage created;
      try {
        container.start();
        admin.start();
        created = new RustfsStorage(container, admin);
        created.setUpUser();
      } catch (RuntimeException e) {
        // Stopping a container that never started does nothing.
        admin.stop();
        container.stop();
        network.close();
        throw e;
      }
      instance = created;
    }
    return instance;
  }

  private void setUpUser() {
    mc("alias", "set", ALIAS, "http://" + HOST + ":" + S3_PORT, ROOT_USER, ROOT_PASSWORD);
    mc("admin", "user", "add", ALIAS, USER, USER_SECRET);
    String policy =
        """
        {"Version": "2012-10-17", "Statement": [
          {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]}
        """;
    admin.copyFileToContainer(Transferable.of(policy, 0644), "/tmp/" + POLICY + ".json");
    mc("admin", "policy", "create", ALIAS, POLICY, "/tmp/" + POLICY + ".json");
    mc("admin", "policy", "attach", ALIAS, POLICY, "--user", USER);
  }

  private void mc(String... args) {
    new McRunner(admin, "--config-dir", "/tmp/mc").run(args);
  }

  @Override
  public String s3Endpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT);
  }

  @Override
  public Optional<String> roleArn() {
    // RustFS ignores the role: its STS issues sessions with the key's own permissions, narrowed by
    // the session policy.
    return Optional.of("arn:aws:iam::123456789012:role/uc-test");
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
  public synchronized void createBucket(String bucket) {
    mc("mb", "--ignore-existing", ALIAS + "/" + bucket);
  }
}
