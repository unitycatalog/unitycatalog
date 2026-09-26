package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.Test;

public class LivezEndpointTest extends BaseServerTest {

  @Test
  public void livezReturns200AndHealthy() {
    AggregatedHttpResponse response = httpGetObservability("/livez");
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(response.contentUtf8()).contains("\"healthy\":true");
  }
}
