package io.unitycatalog.server.service.sharing;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * With server.opensharing.enabled left at false, UC serves none of OpenSharing's routes and touches
 * none of its tables.
 */
public class OpenSharingDisabledTest extends BaseAuthCRUDTest {

  @Test
  public void servesNoSharingRoutes() {
    assertThat(get("/api/1.0/opensharing/shares").status().code()).isEqualTo(404);
    // UC answers any path under its API prefix that no service serves the same way.
    AggregatedHttpResponse unrouted = get("/api/2.1/unity-catalog/no-such-service");
    assertThat(unrouted.status().isSuccess()).isFalse();
    assertThat(get("/api/2.1/unity-catalog/opensharing/provider/shares").status())
        .isEqualTo(unrouted.status());
  }

  @Test
  public void leavesOpenSharingsTablesAlone() throws SQLException {
    // The schema has the tables whether or not OpenSharing is enabled, but nothing may read or
    // write them with it off: the server goes on serving, authorization included, without them.
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection();
        Statement statement = connection.createStatement()) {
      for (String table :
          List.of(
              "os_share_permissions",
              "os_recipient_tokens",
              "os_shared_data_objects",
              "os_share_properties",
              "os_recipients",
              "os_shares")) {
        statement.execute("DROP TABLE " + table);
      }
      connection.commit();
    }

    assertThat(get("/api/2.1/unity-catalog/catalogs").status().code()).isEqualTo(200);
    assertThat(get("/api/2.1/unity-catalog/permissions/metastore/metastore").status().code())
        .isEqualTo(200);
  }

  private AggregatedHttpResponse get(String path) {
    RequestHeaders headers =
        RequestHeaders.builder(HttpMethod.GET, path)
            .add(HttpHeaderNames.AUTHORIZATION, "Bearer " + securityContext.getServiceToken())
            .build();
    return WebClient.of(serverConfig.getServerUrl()).execute(headers).aggregate().join();
  }
}
