package io.unitycatalog.server.sdk.tempcredential;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SdkTemporaryPathCredentialTest extends BaseCRUDTestWithMockCredentials {
  protected SdkTemporaryCredentialOperations temporaryCredentialOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SdkTemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    temporaryCredentialOperations = createTemporaryCredentialsOperations(serverConfig);
  }

  @ParameterizedTest
  @MethodSource("provideTestArguments")
  public void testGenerateTemporaryCredentialsWhereConfIsProvided(
      String scheme, boolean isConfiguredPath) throws ApiException {
    String url = getTestCloudPath(scheme, isConfiguredPath);
    GenerateTemporaryPathCredential generateTemporaryPathCredential =
        new GenerateTemporaryPathCredential().url(url).operation(PathOperation.PATH_READ);
    if (isConfiguredPath) {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialOperations.generateTemporaryPathCredentials(
              generateTemporaryPathCredential);
      assertTemporaryCredentials(temporaryCredentials, scheme);
    } else {
      assertThatThrownBy(
              () ->
                  temporaryCredentialOperations.generateTemporaryPathCredentials(
                      generateTemporaryPathCredential))
          .isInstanceOf(ApiException.class);
    }
  }
}
