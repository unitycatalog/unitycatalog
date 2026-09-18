package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class ReadyzEndpointTest extends BaseServerTest {

  @Test
  @SneakyThrows
  public void readyzReturns200WhenDbReachable() {
    // BaseServerTest starts the server against an H2 in-memory DB, so the synchronous startup
    // probe marks the checker ready before the server serves — no polling needed.
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(serverConfig.getServerUrl() + "/readyz"))
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
  }
}
