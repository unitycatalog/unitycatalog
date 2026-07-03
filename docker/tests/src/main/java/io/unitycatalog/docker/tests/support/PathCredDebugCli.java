package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import java.util.Map;

/** Compare PATH_READ / PATH_READ_WRITE across REST client styles used in docker tests vs Spark. */
public final class PathCredDebugCli {

  private PathCredDebugCli() {}

  public static void main(String[] args) throws Exception {
    String serverUrl = DockerTestConfig.SERVER_URL;
    String sparkServerUrl = DockerTestConfig.SPARK_UC_SERVER_URI;
    String adminToken = DockerTestConfig.adminToken();
    String path =
        DockerTestConfig.DATA_BUCKET + "/tenant/ANOTHERBUCKET/direct/debug_probe";
    if (args.length > 0 && !args[0].startsWith("--")) {
      path = args[0];
    }

    TenantBootstrap.TenantSpec spec =
        TenantBootstrap.TenantSpec.builder()
            .tenantId("ANOTHERBUCKET")
            .catalogName("catanother")
            .userEmail("useranother@example.com")
            .credentialName("cred_anotherbucket")
            .externalLocationName("el_catanother")
            .dataExternalLocationName("el_catanother_data")
            .build();

    System.out.println("==> Bootstrap tenant (idempotent)");
    TenantBootstrap.bootstrap(serverUrl, adminToken, spec);
    String userToken = AuthSupport.ucTokenForUser(serverUrl, spec.userEmail(), null);
    new UcOperations(serverUrl, adminToken)
        .grantExternalLocationReadWrite(spec.dataExternalLocationName(), spec.userEmail());

    System.out.println("==> Path: " + path);
    System.out.println("==> User: " + spec.userEmail());
    System.out.println();

    if (args.length > 0 && "--repeat".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 10;
      repeatProbes(serverUrl, sparkServerUrl, userToken, path, n);
      return;
    }

    if (args.length > 0 && "--ua-matrix".equals(args[0])) {
      userAgentMatrix(serverUrl, userToken, path);
      return;
    }

    if (args.length > 0 && "--header-pad".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      headerPaddingProbe(serverUrl, userToken, path, n);
      return;
    }

    if (args.length > 0 && "--spark-only".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      int ok = 0;
      for (int i = 0; i < n; i++) {
        if (callOk(sparkLikeClient(serverUrl, userToken), path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("spark-like PATH_READ=" + ok + "/" + n);
      return;
    }

    if (args.length > 0 && "--plain-then-spark".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      int delayMs = args.length > 2 ? Integer.parseInt(args[2]) : 0;
      int sparkAfterPlainOk = 0;
      for (int i = 0; i < n; i++) {
        callOk(plainClient(serverUrl, userToken), path, PathOperation.PATH_READ);
        if (delayMs > 0) {
          Thread.sleep(delayMs);
        }
        if (callOk(sparkLikeClient(serverUrl, userToken), path, PathOperation.PATH_READ)) {
          sparkAfterPlainOk++;
        }
      }
      System.out.println("spark-after-plain=" + sparkAfterPlainOk + "/" + n + " delayMs=" + delayMs);
      return;
    }

    if (args.length > 0 && "--spark-then-spark".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      int ok = 0;
      for (int i = 0; i < n; i++) {
        callOk(sparkLikeClient(serverUrl, userToken), path, PathOperation.PATH_READ);
        if (callOk(sparkLikeClient(serverUrl, userToken), path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("spark-after-spark=" + ok + "/" + n);
      return;
    }

    if (args.length > 0 && "--plain-then-plain".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      int ok = 0;
      ApiClient plain = plainClient(serverUrl, userToken);
      ApiClient plain2 = plainClient(serverUrl, userToken);
      for (int i = 0; i < n; i++) {
        callOk(plain, path, PathOperation.PATH_READ);
        if (callOk(plain2, path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("plain2-after-plain1=" + ok + "/" + n);
      return;
    }

    if (args.length > 0 && "--plain-then-long-ua".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 50;
      ApiClient longUa =
          ApiClientBuilder.create()
              .uri(java.net.URI.create(serverUrl))
              .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", userToken)))
              .addAppVersion("PaddingApp", "1.2.3.4.5.6.7.8.9.0-extra-long-version-string")
              .addAppVersion("AnotherApp", "2.0.0")
              .build();
      int ok = 0;
      for (int i = 0; i < n; i++) {
        callOk(plainClient(serverUrl, userToken), path, PathOperation.PATH_READ);
        if (callOk(longUa, path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("long-ua-after-plain=" + ok + "/" + n);
      return;
    }

    if (args.length > 0 && "--same-client-ua-toggle".equals(args[0])) {
      int n = args.length > 1 ? Integer.parseInt(args[1]) : 100;
      java.util.concurrent.atomic.AtomicBoolean longUa = new java.util.concurrent.atomic.AtomicBoolean(false);
      ApiClient client =
          ApiClientBuilder.create()
              .uri(java.net.URI.create(serverUrl))
              .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", userToken)))
              .addRequestInterceptor(
                  builder ->
                      builder.setHeader(
                          "User-Agent",
                          longUa.get()
                              ? "UnityCatalog-Java-Client/0.5.0-SNAPSHOT Spark/4.1.2 Delta/4.2.0"
                              : "UnityCatalog-Java-Client/0.5.0-SNAPSHOT"))
              .build();
      int ok = 0;
      for (int i = 0; i < n; i++) {
        longUa.set(false);
        callOk(client, path, PathOperation.PATH_READ);
        longUa.set(true);
        if (callOk(client, path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("long-ua-after-short-same-connection=" + ok + "/" + n);
      return;
    }

    for (PathOperation op : new PathOperation[] {PathOperation.PATH_READ, PathOperation.PATH_READ_WRITE}) {
      System.out.println("--- " + op + " ---");
      probe("plain REST (UcClientFactory)", plainClient(serverUrl, userToken), path, op);
      probe("REST + app versions (Spark-like)", sparkLikeClient(serverUrl, userToken), path, op);
      probe(
          "REST + app versions @ host.docker.internal",
          sparkLikeClient(sparkServerUrl, userToken),
          path,
          op);
      System.out.println();
    }

    System.out.println("==> Spark JDBC after seed write");
    long ts = System.currentTimeMillis() / 1000;
    String seedPath =
        DockerTestConfig.DATA_BUCKET + "/tenant/ANOTHERBUCKET/direct/debug_" + ts;
    DockerCompose.upSparkStack(spec.catalogName(), userToken);
    try {
      SparkJdbcClient.execute(
          spec.catalogName(),
          userToken,
          "s1",
          "INSERT OVERWRITE DIRECTORY '"
              + seedPath
              + "' USING parquet SELECT cast(id AS int) AS id FROM range(5)");
      System.out.println("  write: OK");
    } catch (Exception e) {
      System.out.println("  write: FAIL " + rootMessage(e));
    }
    try {
      String out =
          SparkJdbcClient.execute(
              spec.catalogName(),
              userToken,
              "s1",
              "SELECT count(*) AS c FROM parquet.`" + seedPath + "`");
      System.out.println("  read:  OK output=" + out.strip());
    } catch (Exception e) {
      System.out.println("  read:  FAIL " + rootMessage(e));
    }
  }

  private static ApiClient plainClient(String serverUrl, String token) {
    return UcClientFactory.catalogClient(serverUrl, token);
  }

  /** Mirrors Spark ApiClientFactory: adds Spark/Delta/Java/Scala versions to User-Agent. */
  private static ApiClient sparkLikeClient(String serverUrl, String token) {
    ApiClientBuilder builder =
        ApiClientBuilder.create()
            .uri(java.net.URI.create(serverUrl))
            .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
            .addAppVersion("Spark", "4.1.2")
            .addAppVersion("Delta", "4.2.0")
            .addAppVersion("Java", System.getProperty("java.version"))
            .addAppVersion("Scala", "2.13.16");
    return builder.build();
  }

  private static ApiClient clientWithExtraApp(String serverUrl, String token, String app, String ver) {
    return ApiClientBuilder.create()
        .uri(java.net.URI.create(serverUrl))
        .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
        .addAppVersion(app, ver)
        .build();
  }

  /**
   * Extra request headers increase the chance Armeria delivers the JSON body in multiple peekData
   * chunks on servers missing the UnityAccessDecorator payload guard (a5ac9aea).
   */
  private static void headerPaddingProbe(String serverUrl, String token, String path, int n) {
    System.out.println("==> Header padding probe x" + n + " (PATH_READ)");
    int plainOk = 0;
    int paddedOk = 0;
    for (int i = 0; i < n; i++) {
      if (callOk(plainClient(serverUrl, token), path, PathOperation.PATH_READ)) plainOk++;
      if (callOk(paddedClient(serverUrl, token), path, PathOperation.PATH_READ)) paddedOk++;
    }
    System.out.println("plain=" + plainOk + "/" + n + " padded-headers=" + paddedOk + "/" + n);
  }

  private static ApiClient paddedClient(String serverUrl, String token) {
    String pad = "X".repeat(256);
    return ApiClientBuilder.create()
        .uri(java.net.URI.create(serverUrl))
        .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
        .addRequestInterceptor(builder -> builder.header("X-Padding", pad))
        .build();
  }

  private static void userAgentMatrix(String serverUrl, String token, String path) {
    System.out.println("==> User-Agent component matrix (5 calls each, PATH_READ)");
    record Case(String label, ApiClient client) {}
    Case[] cases =
        new Case[] {
          new Case("default only", plainClient(serverUrl, token)),
          new Case("+ Spark", clientWithExtraApp(serverUrl, token, "Spark", "4.1.2")),
          new Case("+ Delta", clientWithExtraApp(serverUrl, token, "Delta", "4.2.0")),
          new Case("full Spark-like", sparkLikeClient(serverUrl, token)),
        };
    for (Case c : cases) {
      int ok = 0;
      for (int i = 0; i < 5; i++) {
        if (callOk(c.client(), path, PathOperation.PATH_READ)) ok++;
      }
      System.out.println("  " + c.label() + ": " + ok + "/5");
    }
  }

  private static void probe(String label, ApiClient client, String path, PathOperation op) {
    try {
      new TemporaryCredentialsApi(client)
          .generateTemporaryPathCredentials(
              new GenerateTemporaryPathCredential().url(path).operation(op));
      System.out.println("  " + label + ": OK");
    } catch (ApiException e) {
      System.out.println(
          "  " + label + ": " + e.getCode() + " " + truncate(e.getResponseBody(), 200));
    }
  }

  private static void repeatProbes(
      String localhost, String dockerHost, String token, String path, int n) throws Exception {
    System.out.println("==> Repeat probe x" + n + " (single token, no re-bootstrap between calls)");
    int plainReadOk = 0, sparkReadOk = 0, plainRwOk = 0, sparkRwOk = 0;
    for (int i = 1; i <= n; i++) {
      if (callOk(plainClient(localhost, token), path, PathOperation.PATH_READ)) plainReadOk++;
      if (callOk(sparkLikeClient(localhost, token), path, PathOperation.PATH_READ)) sparkReadOk++;
      if (callOk(plainClient(localhost, token), path, PathOperation.PATH_READ_WRITE)) plainRwOk++;
      if (callOk(sparkLikeClient(localhost, token), path, PathOperation.PATH_READ_WRITE)) sparkRwOk++;
    }
    System.out.println(
        "PATH_READ       plain=" + plainReadOk + "/" + n + " spark-like=" + sparkReadOk + "/" + n);
    System.out.println(
        "PATH_READ_WRITE plain=" + plainRwOk + "/" + n + " spark-like=" + sparkRwOk + "/" + n);
  }

  private static boolean callOk(ApiClient client, String path, PathOperation op) {
    try {
      new TemporaryCredentialsApi(client)
          .generateTemporaryPathCredentials(
              new GenerateTemporaryPathCredential().url(path).operation(op));
      return true;
    } catch (ApiException e) {
      System.out.println(
          "  FAIL " + op + " code=" + e.getCode() + " body=" + truncate(e.getResponseBody(), 120));
      return false;
    }
  }

  private static String rootMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return msg == null ? cur.getClass().getSimpleName() : truncate(msg, 400);
  }

  private static String truncate(String s, int max) {
    if (s == null) {
      return "";
    }
    return s.length() <= max ? s : s.substring(0, max) + "...";
  }
}
