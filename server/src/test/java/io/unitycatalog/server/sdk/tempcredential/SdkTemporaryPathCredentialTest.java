package io.unitycatalog.server.sdk.tempcredential;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SdkTemporaryPathCredentialTest extends BaseCRUDTestWithMockCredentials {
  private TemporaryCredentialsApi temporaryCredentialsApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    temporaryCredentialsApi = new TemporaryCredentialsApi(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties serverProperties) {
    super.setUpCredentialOperations(serverProperties);
    cloudCredentialVendor = spy(cloudCredentialVendor);
  }

  @Test
  public void testPendingCleanupPathsAreRejectedBeforeCredentialVending() throws ApiException {
    String cleanupPath = AWS_EXTERNAL_LOCATION_PATH + "/deleted";
    createCleanupTask(cleanupPath);
    clearInvocations(cloudCredentialVendor);

    for (String url :
        List.of(AWS_EXTERNAL_LOCATION_PATH, cleanupPath, cleanupPath + "/data/file.parquet")) {
      TestUtils.assertApiException(
          () ->
              temporaryCredentialsApi.generateTemporaryPathCredentials(
                  new GenerateTemporaryPathCredential()
                      .url(url)
                      .operation(PathOperation.PATH_READ)),
          ErrorCode.PERMISSION_DENIED,
          "Input path overlaps pending storage cleanup");
    }
    verify(cloudCredentialVendor, never()).vendCredential(any());

    String nonOverlappingPath = AWS_EXTERNAL_LOCATION_PATH + "/active";
    temporaryCredentialsApi.generateTemporaryPathCredentials(
        new GenerateTemporaryPathCredential()
            .url(nonOverlappingPath)
            .operation(PathOperation.PATH_READ));
    verify(cloudCredentialVendor).vendCredential(any());
  }

  private void createCleanupTask(String storageLocation) {
    StorageCleanupTaskRepository repository =
        new StorageCleanupTaskRepository(hibernateConfigurator.getSessionFactory());
    TransactionManager.executeWithTransaction(
        hibernateConfigurator.getSessionFactory(),
        session -> {
          repository.create(
              session, ResourceType.TABLE, UUID.randomUUID(), "orders", storageLocation);
          return null;
        },
        "Failed to create test cleanup task",
        /* readOnly= */ false);
  }

  @ParameterizedTest
  @MethodSource("getArgumentsForParameterizedTests")
  public void testGenerateTemporaryCredentialsWhereConfIsProvided(
      String scheme, boolean isConfiguredPath) throws ApiException {
    String url = getTestCloudPath(scheme, isConfiguredPath);
    GenerateTemporaryPathCredential generateTemporaryPathCredential =
        new GenerateTemporaryPathCredential().url(url).operation(PathOperation.PATH_READ);
    if (isConfiguredPath) {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryPathCredentials(generateTemporaryPathCredential);
      assertTemporaryCredentials(temporaryCredentials, scheme, url);
    } else {
      assertThatThrownBy(
              () ->
                  temporaryCredentialsApi.generateTemporaryPathCredentials(
                      generateTemporaryPathCredential))
          .isInstanceOf(ApiException.class);
    }
  }

  @Test
  public void testGenerateAwsTemporaryCredentialsFromMasterRole() throws ApiException {
    for (String url : List.of(AWS_EXTERNAL_LOCATION_PATH, AWS_EXTERNAL_LOCATION_PATH + "/table1")) {
      GenerateTemporaryPathCredential generateTemporaryPathCredential =
          new GenerateTemporaryPathCredential().url(url).operation(PathOperation.PATH_READ_WRITE);
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryPathCredentials(generateTemporaryPathCredential);
      EchoAwsStsClient.assertAwsCredential(temporaryCredentials);
    }
    // Should fail because the path is not covered by external location
    TestUtils.assertApiException(
        () ->
            temporaryCredentialsApi.generateTemporaryPathCredentials(
                new GenerateTemporaryPathCredential()
                    .url(AWS_EXTERNAL_LOCATION_PARENT_PATH)
                    .operation(PathOperation.PATH_READ_WRITE)),
        ErrorCode.FAILED_PRECONDITION,
        "S3 bucket configuration not found");
  }
}
