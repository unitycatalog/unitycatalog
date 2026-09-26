package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.Test;

public class MetricsEndpointTest extends BaseServerTest {

  @Test
  public void metricsReturns200WithJvmSeries() {
    AggregatedHttpResponse response = httpGetObservability("/metrics");
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(response.contentUtf8()).contains("jvm_memory_used_bytes");
  }
}
