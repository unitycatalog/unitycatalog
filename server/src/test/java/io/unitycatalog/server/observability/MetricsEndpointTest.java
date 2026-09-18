package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class MetricsEndpointTest extends BaseServerTest {

  @Test
  @SneakyThrows
  public void metricsReturns200WithJvmSeries() {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(serverConfig.getServerUrl() + "/metrics"))
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("jvm_memory_used_bytes");
  }
}
