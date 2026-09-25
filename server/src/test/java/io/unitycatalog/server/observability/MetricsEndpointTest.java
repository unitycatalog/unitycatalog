package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

public class MetricsEndpointTest extends BaseServerTest {

  @Test
  public void metricsReturns200WithJvmSeries() {
    HttpResponse<String> response = httpGet("/metrics");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("jvm_memory_used_bytes");
  }
}
