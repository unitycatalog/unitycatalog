package io.unitycatalog.docker.tests.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Start MinIO + Spark Thrift Server via docker compose (SQL runs separately over JDBC). */
public final class DockerCompose {

  private static volatile String activeCatalog;
  private static volatile String activeToken;

  private DockerCompose() {}

  public static void upSparkStack(String catalog, String userToken)
      throws IOException, InterruptedException {
    if (catalog.equals(activeCatalog) && userToken.equals(activeToken)) {
      return;
    }
    run(
        "up",
        "-d",
        "--remove-orphans",
        "minio",
        "minio-init");
    run(
        Map.of("UC_CATALOG", catalog, "UC_AUTH_TOKEN", userToken),
        "up",
        "-d",
        "--force-recreate",
        "--wait",
        "spark-thrift");
    SparkJdbcClient.waitForPort();
    activeCatalog = catalog;
    activeToken = userToken;
  }

  private static void run(String... args) throws IOException, InterruptedException {
    run(Map.of(), args);
  }

  private static void run(Map<String, String> env, String... args)
      throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add("docker");
    command.add("compose");
    command.add("--profile");
    command.add("spark");
    command.add("-f");
    command.add(DockerTestConfig.COMPOSE_FILE.toString());
    command.addAll(List.of(args));

    ProcessBuilder builder = new ProcessBuilder(command);
    builder.directory(DockerTestConfig.REPO_ROOT.toFile());
    builder.redirectErrorStream(true);
    builder.environment().putAll(env);

    System.err.println("==> docker compose " + String.join(" ", args));
    Process process = builder.start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exit = process.waitFor();
    if (exit != 0) {
      throw new IOException(
          "docker compose " + String.join(" ", args) + " failed (" + exit + "):\n" + output);
    }
  }
}
