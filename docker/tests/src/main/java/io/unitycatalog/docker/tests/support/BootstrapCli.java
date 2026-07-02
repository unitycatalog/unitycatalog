package io.unitycatalog.docker.tests.support;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** CLI entry point for {@code docker/tests/ucJava.sh bootstrap_tenant_java}. */
public final class BootstrapCli {

  private BootstrapCli() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || contains(args, "-h") || contains(args, "--help")) {
      System.out.println(
          """
          Usage: bootstrapTenantViaRest.sh --tenant-id ID --catalog NAME --user-email EMAIL [options]

          Options mirror the former BootstrapTenant tool (--password, --credential-name, etc.).
          """);
      System.exit(args.length == 0 ? 1 : 0);
    }

    Map<String, String> values = parseArgs(args);
    String adminToken = values.get("admin-token");
    if (adminToken == null || adminToken.isBlank()) {
      Path tokenFile = Path.of(values.getOrDefault("admin-token-file", "etc/conf/token.txt"));
      adminToken = Files.readString(tokenFile).trim();
    }

    String catalog = required(values, "catalog");
    TenantBootstrap.TenantSpec spec =
        TenantBootstrap.TenantSpec.builder()
            .tenantId(required(values, "tenant-id"))
            .catalogName(catalog)
            .userEmail(required(values, "user-email"))
            .userPassword(values.get("password"))
            .credentialName(values.getOrDefault("credential-name", "cred_tenant"))
            .externalLocationName(values.getOrDefault("external-location", "el_" + catalog))
            .dataExternalLocationName(
                values.getOrDefault("data-external-location", "el_" + catalog + "_data"))
            .bucket(values.getOrDefault("bucket", DockerTestConfig.BUCKET))
            .dataBucket(values.getOrDefault("data-bucket", DockerTestConfig.DATA_BUCKET))
            .storageRoleArn(
                values.getOrDefault("storage-role-arn", DockerTestConfig.STORAGE_ROLE_ARN))
            .build();

    String serverUrl = values.getOrDefault("server-url", DockerTestConfig.SERVER_URL);
    TenantBootstrap.bootstrap(serverUrl, adminToken, spec);
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (!arg.startsWith("--")) {
        throw new IllegalArgumentException("Unknown argument: " + arg);
      }
      String key = arg.substring(2);
      if ("help".equals(key)) {
        continue;
      }
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("Missing value for " + arg);
      }
      values.put(key, args[++i]);
    }
    return values;
  }

  private static String required(Map<String, String> values, String key) {
    String value = values.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("--" + key + " is required");
    }
    return value;
  }

  private static boolean contains(String[] args, String flag) {
    for (String arg : args) {
      if (flag.equals(arg)) {
        return true;
      }
    }
    return false;
  }
}
