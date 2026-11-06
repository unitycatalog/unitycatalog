package io.unitycatalog.server.sharing;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import org.junit.jupiter.api.Test;

/** With server.opensharing.enabled left at false, UC serves none of OpenSharing's routes. */
public class OpenSharingDisabledTest extends BaseAuthCRUDTest {

  @Test
  public void servesNoSharingRoutes() {
    // UC answers any path under its API prefix that no service serves the same way.
    AggregatedHttpResponse unrouted = get("/api/2.1/unity-catalog/no-such-service");
    assertThat(unrouted.status().isSuccess()).isFalse();
    assertThat(get("/api/2.1/unity-catalog/opensharing/provider/shares").status())
        .isEqualTo(unrouted.status());
  }

  private AggregatedHttpResponse get(String path) {
    RequestHeaders headers =
        RequestHeaders.builder(HttpMethod.GET, path)
            .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + securityContext.getServiceToken())
            .build();
    return WebClient.of(serverConfig.getServerUrl()).execute(headers).aggregate().join();
  }
}
