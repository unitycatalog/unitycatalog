package io.unitycatalog.server.base.metastore;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseMetastoreSummaryTest extends BaseCRUDTest {
  protected MetastoreOperations metastoreOperations;

  protected abstract MetastoreOperations createMetastoreOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    metastoreOperations = createMetastoreOperations(serverConfig);
  }

  @Test
  public void testMetastoreSummary() throws ApiException {
    GetMetastoreSummaryResponse metastoreSummary = metastoreOperations.getMetastoreSummary();
    String metastoreId1 = metastoreSummary.getMetastoreId();
    assertThat(metastoreId1).isNotNull();

    // Verify that the metastore ID is stable
    GetMetastoreSummaryResponse metastoreSummary2 = metastoreOperations.getMetastoreSummary();
    String metastoreId2 = metastoreSummary2.getMetastoreId();
    assertThat(metastoreId2).isNotNull();
    assertThat(metastoreId1).isEqualTo(metastoreId2);

    // Verify that the metastore ID is stable even after a server restart
    unityCatalogServer.stop();
    unityCatalogServer.start();
    GetMetastoreSummaryResponse metastoreSummary3 = metastoreOperations.getMetastoreSummary();
    String metastoreId3 = metastoreSummary3.getMetastoreId();
    assertThat(metastoreId3).isNotNull();
    assertThat(metastoreId1).isEqualTo(metastoreId3);
  }
}
