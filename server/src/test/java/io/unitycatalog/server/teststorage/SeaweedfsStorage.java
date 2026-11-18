package io.unitycatalog.server.teststorage;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * SeaweedFS in a container, one per JVM: master, volume server, filer and S3 gateway in one {@code
 * weed server}. An admin identity only creates buckets. Unity Catalog's credential holds the key of
 * a user that may read and write objects and assume a role whose policy allows all S3 actions; the
 * session policy Unity Catalog sends with AssumeRole narrows each vended credential to its path.
 */
final class SeaweedfsStorage implements TestStorage {

  private static final int S3_PORT = 8333;
  private static final String ADMIN = "uc-test-admin";
  private static final String ADMIN_SECRET = "uc-test-admin-value";
  private static final String USER = "uc-test-user";
  private static final String USER_SECRET = "uc-test-user-value";
  private static final String ACCOUNT = "000000000000";
  private static final String ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/uc-test";
  private static final String CONFIG_DIR = "/etc/uc";

  private static SeaweedfsStorage instance;

  private final GenericContainer<?> container;

  private SeaweedfsStorage(GenericContainer<?> container) {
    this.container = container;
  }

  /** The SeaweedFS container, started on first use. */
  static synchronized SeaweedfsStorage get() {
    if (instance == null) {
      String image = SupportedStorage.SEAWEEDFS.image().orElseThrow();
      GenericContainer<?> container =
          new GenericContainer<>(DockerImageName.parse(image))
              .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("weed"))
              .withCommand(
                  "server",
                  "-dir=/data",
                  // Each bucket takes 7 volumes on its first write, so the default of 8 volume
                  // slots runs out after one bucket; small volumes keep many buckets cheap.
                  "-volume.max=100",
                  "-master.volumeSizeLimitMB=64",
                  "-s3",
                  "-s3.port=" + S3_PORT,
                  "-s3.config=" + CONFIG_DIR + "/s3.json",
                  "-s3.iam.config=" + CONFIG_DIR + "/iam.json")
              .withCopyToContainer(Transferable.of(identities()), CONFIG_DIR + "/s3.json")
              .withCopyToContainer(Transferable.of(iam()), CONFIG_DIR + "/iam.json")
              .withExposedPorts(S3_PORT)
              .waitingFor(Wait.forListeningPort());
      SeaweedfsStorage created;
      try {
        container.start();
        created = new SeaweedfsStorage(container);
        // The master allocates no volumes for about 15 seconds after it becomes leader, so a first
        // write waits that long. Waiting here keeps it out of the tests, where it would outlast the
        // server's request timeout.
        S3Buckets.awaitWritable(created.s3Endpoint(), ADMIN, ADMIN_SECRET, "uc-test-ready");
      } catch (RuntimeException e) {
        container.stop();
        throw e;
      }
      instance = created;
    }
    return instance;
  }

  /** The static identities: the bucket admin, and the user whose key Unity Catalog holds. */
  private static String identities() {
    return """
    {"identities": [
      {"name": "%s", "credentials": [{"accessKey": "%s", "secretKey": "%s"}],
       "actions": ["Admin", "Read", "List", "Write", "Tagging"]},
      {"name": "%s", "credentials": [{"accessKey": "%s", "secretKey": "%s"}],
       "actions": ["Read", "List", "Write"]}]}
    """
        .formatted(ADMIN, ADMIN, ADMIN_SECRET, USER, USER, USER_SECRET);
  }

  /** The STS settings, and the role the user may assume. */
  private static String iam() {
    // At least 32 bytes, base64-encoded; signs the session tokens of this test container only.
    String signingKey =
        Base64.getEncoder()
            .encodeToString("uc-test-seaweedfs-sts-signing-key".getBytes(StandardCharsets.UTF_8));
    return """
    {"sts": {"tokenDuration": "1h", "maxSessionLength": "12h", "issuer": "seaweedfs-sts",
             "signingKey": "%s"},
     "policy": {"defaultEffect": "Deny", "storeType": "memory"},
     "policies": [{"name": "uc-test-rw", "document": {"Version": "2012-10-17", "Statement": [
       {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]}}],
     "roles": [{"roleName": "uc-test", "roleArn": "%s", "attachedPolicies": ["uc-test-rw"],
       "trustPolicy": {"Version": "2012-10-17", "Statement": [
         {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::%s:user/%s"},
          "Action": ["sts:AssumeRole"]}]}}]}
    """
        .formatted(signingKey, ROLE_ARN, ACCOUNT, USER);
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
    return Optional.of(ROLE_ARN);
  }

  @Override
  public synchronized void createBucket(String bucket) {
    S3Buckets.create(s3Endpoint(), ADMIN, ADMIN_SECRET, bucket);
  }
}
