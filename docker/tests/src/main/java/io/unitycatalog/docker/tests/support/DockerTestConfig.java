package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DockerTestConfig {

  public static final String SERVER_URL =
      System.getenv().getOrDefault("UC_SERVER_URL", "http://localhost:8080");
  public static final String BUCKET =
      System.getenv().getOrDefault("BUCKET", "s3://unity-catalog");
  public static final String DATA_BUCKET =
      System.getenv().getOrDefault("DATA_BUCKET", "s3://data");
  public static final String STORAGE_ROLE_ARN =
      System.getenv()
          .getOrDefault(
              "STORAGE_ROLE_ARN", "arn:aws:iam::123456789012:role/uc-tenant-storage");
  public static final Path REPO_ROOT = resolveRepoRoot();
  public static final Path COMPOSE_FILE = REPO_ROOT.resolve("docker/compose.yaml");
  public static final Path ADMIN_TOKEN_FILE = REPO_ROOT.resolve("etc/conf/token.txt");
  public static final String SPARK_JDBC_HOST =
      System.getenv().getOrDefault("SPARK_JDBC_HOST", "localhost");
  public static final int SPARK_JDBC_PORT =
      Integer.parseInt(System.getenv().getOrDefault("SPARK_JDBC_PORT", "10000"));

  private DockerTestConfig() {}

  public static String adminToken() throws IOException {
    String fromEnv = System.getenv("UC_ADMIN_TOKEN");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv.trim();
    }
    if (!Files.isRegularFile(ADMIN_TOKEN_FILE)) {
      throw new IOException("Admin token missing: set UC_ADMIN_TOKEN or create " + ADMIN_TOKEN_FILE);
    }
    return Files.readString(ADMIN_TOKEN_FILE).trim();
  }

  public static boolean keepTenants() {
    return "true".equalsIgnoreCase(System.getenv("DOCKER_TESTS_KEEP"));
  }

  public static boolean isServerReachable(String adminToken) {
    try {
      new CatalogsApi(UcClientFactory.catalogClient(SERVER_URL, adminToken)).listCatalogs(null, 1);
      return true;
    } catch (ApiException e) {
      return false;
    }
  }

  public static String tenantPath(String bucket, String tenantId) {
    return bucket + "/tenant/" + tenantId;
  }

  private static Path resolveRepoRoot() {
    String fromEnv = System.getenv("UC_REPO_ROOT");
    if (fromEnv != null && !fromEnv.isBlank()) {
      return Path.of(fromEnv).toAbsolutePath().normalize();
    }
    return Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
  }
}
