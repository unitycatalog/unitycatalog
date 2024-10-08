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
    assertThat(metastoreSummary.getName()).isNotNull();
    // Verify that it is stable
    String metastoreId2 = metastoreSummary.getMetastoreId();
    assertThat(metastoreId2).isNotNull();
    assertThat(metastoreId1).isEqualTo(metastoreId2);
  }
}
