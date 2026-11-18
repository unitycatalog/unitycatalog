package io.unitycatalog.server.teststorage;

import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * MinIO in a container, one per JVM. The root user only sets it up: Unity Catalog's credential
 * holds a separate storage user whose policy allows all S3 actions, and the session policy Unity
 * Catalog sends with AssumeRole narrows each vended credential to its path.
 */
final class MinioStorage implements TestStorage {

  private static final int S3_PORT = 9000;
  private static final String ROOT_USER = "uc-test-admin";
  private static final String ROOT_PASSWORD = "uc-test-admin-value";
  private static final String USER = "uc-test-user";
  private static final String USER_SECRET = "uc-test-user-value";
  private static final String POLICY = "uc-test-rw";
  private static final String ALIAS = "local";

  private static MinioStorage instance;

  private final GenericContainer<?> container;

  private MinioStorage(GenericContainer<?> container) {
    this.container = container;
  }

  /** The MinIO container, started and set up on first use. */
  static synchronized MinioStorage get() {
    if (instance == null) {
      String image = SupportedStorage.MINIO.image().orElseThrow();
      GenericContainer<?> container =
          new GenericContainer<>(DockerImageName.parse(image))
              .withExposedPorts(S3_PORT)
              .withEnv("MINIO_ROOT_USER", ROOT_USER)
              .withEnv("MINIO_ROOT_PASSWORD", ROOT_PASSWORD)
              .waitingFor(Wait.forHttp("/minio/health/live").forPort(S3_PORT));
      MinioStorage created;
      try {
        container.start();
        created = new MinioStorage(container);
        created.setUpUser();
      } catch (RuntimeException e) {
        container.stop();
        throw e;
      }
      instance = created;
    }
    return instance;
  }

  private void setUpUser() {
    mc("alias", "set", ALIAS, "http://127.0.0.1:" + S3_PORT, ROOT_USER, ROOT_PASSWORD);
    mc("admin", "user", "add", ALIAS, USER, USER_SECRET);
    String policy =
        """
        {"Version": "2012-10-17", "Statement": [
          {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]}
        """;
    container.copyFileToContainer(Transferable.of(policy), "/tmp/" + POLICY + ".json");
    mc("admin", "policy", "create", ALIAS, POLICY, "/tmp/" + POLICY + ".json");
    mc("admin", "policy", "attach", ALIAS, POLICY, "--user", USER);
  }

  private void mc(String... args) {
    new McRunner(container).run(args);
  }

  @Override
  public String s3Endpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(S3_PORT);
  }

  @Override
  public Optional<String> roleArn() {
    // MinIO ignores the role: its STS issues sessions with the key's own permissions, narrowed by
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
