package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.Test;

public class ReadyzEndpointTest extends BaseServerTest {

  @Test
  public void readyzReturns200WhenDbReachable() {
    // BaseServerTest starts the server against an H2 in-memory DB, so the synchronous startup
    // probe marks the checker ready before the server serves — no polling needed.
    AggregatedHttpResponse response = httpGetObservability("/readyz");
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(response.contentUtf8()).contains("\"healthy\":true");
  }
}
