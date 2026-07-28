package io.unitycatalog.server.sdk.models;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME2;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.MODEL_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME2;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.ListRegisteredModelsResponse;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.model.BaseModelCRUDTest;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SdkModelCRUDTest extends BaseModelCRUDTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected ModelOperations createModelOperations(ServerConfig config) {
    localRegisteredModelsApi = new RegisteredModelsApi(TestUtils.createApiClient(config));
    return new SdkModelOperations(TestUtils.createApiClient(config));
  }

  /**
   * ModelOperations drops the next page token, so the listing tests below use a direct
   * `RegisteredModelsApi` client to inspect the response object.
   */
  private RegisteredModelsApi localRegisteredModelsApi;

  @Test
  public void testPagingCoversModelsThatShareAName() throws ApiException {
    // Model names are only unique per schema, so a name keyed cursor would skip one of these.
    createCommonResources();
    createModel(CATALOG_NAME, SCHEMA_NAME, MODEL_NAME);
    createModel(CATALOG_NAME2, SCHEMA_NAME2, MODEL_NAME);
    createModel(CATALOG_NAME, SCHEMA_NAME, "uc_testmodel_two");

    ListRegisteredModelsResponse firstPage = listAllModels(1, null);
    ListRegisteredModelsResponse secondPage = listAllModels(1, firstPage.getNextPageToken());
    ListRegisteredModelsResponse thirdPage = listAllModels(1, secondPage.getNextPageToken());

    assertThat(collectFullNames(firstPage, secondPage, thirdPage))
        .containsExactlyInAnyOrder(
            CATALOG_NAME + "." + SCHEMA_NAME + "." + MODEL_NAME,
            CATALOG_NAME2 + "." + SCHEMA_NAME2 + "." + MODEL_NAME,
            CATALOG_NAME + "." + SCHEMA_NAME + ".uc_testmodel_two");

    TestUtils.assertApiException(
        () -> listAllModels(-1, null),
        ErrorCode.INVALID_ARGUMENT,
        "maxResults must be greater than or equal to 0");
    TestUtils.assertApiException(
        () -> listAllModels(1, "not_a_page_token"),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid page token received: not_a_page_token");
  }

  private ListRegisteredModelsResponse listAllModels(Integer maxResults, String pageToken)
      throws ApiException {
    return localRegisteredModelsApi.listRegisteredModels(null, null, maxResults, pageToken);
  }

  private List<String> collectFullNames(ListRegisteredModelsResponse... pages) {
    List<String> fullNames = new ArrayList<>();
    for (ListRegisteredModelsResponse page : pages) {
      for (RegisteredModelInfo model : page.getRegisteredModels()) {
        fullNames.add(model.getFullName());
      }
    }
    return fullNames;
  }

  private void createModel(String catalogName, String schemaName, String modelName)
      throws ApiException {
    localRegisteredModelsApi.createRegisteredModel(
        new CreateRegisteredModel()
            .name(modelName)
            .catalogName(catalogName)
            .schemaName(schemaName)
            .comment(COMMENT));
  }
}
