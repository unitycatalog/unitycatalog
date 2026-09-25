package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

public class LivezEndpointTest extends BaseServerTest {

  @Test
  public void livezReturns200AndHealthy() {
    HttpResponse<String> response = httpGet("/livez");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"healthy\":true");
  }
}
