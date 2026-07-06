package io.unitycatalog.docker.tests.support;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * HTTP clients for OAuth. Configures {@code jdk.net.hosts.file} so {@code {team}.{realm}}
 * resolves to loopback without editing system {@code /etc/hosts}.
 */
public final class OAuthHttp {

  private static final boolean HOSTS_CONFIGURED = configureLocalHostsFile();

  private OAuthHttp() {}

  public static HttpClient createHttpClient() {
    ensureHostsConfigured();
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
  }

  public static WebClient.Builder webClientBuilder() {
    ensureHostsConfigured();
    return WebClient.builder();
  }

  private static void ensureHostsConfigured() {
    if (!HOSTS_CONFIGURED) {
      throw new IllegalStateException(
          "Local OAuth host mapping is not configured; set UC_OAUTH_RESOLVE_LOCAL=0 for"
              + " external OAuth or ensure UC_OAUTH_TEAM/UC_OAUTH_REALM are set");
    }
  }

  private static boolean configureLocalHostsFile() {
    if ("0".equals(System.getenv("UC_OAUTH_RESOLVE_LOCAL"))) {
      return true;
    }
    OAuthTenant tenant = OAuthTenant.fromEnv();
    if (!usesLocalOAuthEndpoint(tenant)) {
      return true;
    }
    try {
      Path hosts = Files.createTempFile("uc-oauth-hosts-", ".txt");
      hosts.toFile().deleteOnExit();
      Files.writeString(
          hosts,
          "127.0.0.1 localhost\n"
              + "127.0.0.1 "
              + tenant.host()
              + "\n");
      System.setProperty("jdk.net.hosts.file", hosts.toString());
      return true;
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static boolean usesLocalOAuthEndpoint(OAuthTenant tenant) {
    String base = tenant.httpBaseUrl();
    if (!base.startsWith("http://")) {
      return false;
    }
    return base.contains(tenant.host() + ":9010");
  }
}
