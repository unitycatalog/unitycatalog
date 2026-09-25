package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

public class ReadyzEndpointTest extends BaseServerTest {

  @Test
  public void readyzReturns200WhenDbReachable() {
    // BaseServerTest starts the server against an H2 in-memory DB, so the synchronous startup
    // probe marks the checker ready before the server serves — no polling needed.
    HttpResponse<String> response = httpGet("/readyz");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"healthy\":true");
  }
}
