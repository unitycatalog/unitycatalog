package io.unitycatalog.server.base.tempcredential;

import static io.unitycatalog.server.utils.TestUtils.*;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.ModelVersionStatus;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class BaseTemporaryModelVersionCredentialTest extends BaseCRUDTestWithMockCredentials {

  protected SchemaOperations schemaOperations;
  protected ModelOperations modelOperations;
  protected TemporaryCredentialOperations temporaryCredentialOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract ModelOperations createModelOperations(ServerConfig serverConfig);

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
    temporaryCredentialOperations = createTemporaryCredentialsOperations(serverConfig);
  }

  protected void createNonFileModelVersion(
      String modelId, long version, ModelVersionStatus status, String storageLocation) {
    long createTime = 1L;
    String modelVersionId = UUID.randomUUID().toString();
    ModelVersionInfo modelVersionInfo =
        new ModelVersionInfo()
            .id(modelVersionId)
            .modelName(MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .runId(MV_RUNID2)
            .source(MV_SOURCE2)
            .status(status)
            .version(version)
            .storageLocation(storageLocation)
            .comment(COMMENT)
            .createdAt(createTime)
            .updatedAt(createTime);
    Transaction tx;
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      tx = session.beginTransaction();
      try {
        ModelVersionInfoDAO modelVersionInfoDAO = ModelVersionInfoDAO.from(modelVersionInfo);
        modelVersionInfoDAO.setRegisteredModelId(UUID.fromString(modelId));
        session.persist(modelVersionInfoDAO);
        tx.commit();
        session.close();
      } catch (RuntimeException e) {
        if (tx != null && tx.getStatus().canRollback()) {
          tx.rollback();
        }
        throw e;
      }
    } catch (RuntimeException e) {
      if (e instanceof BaseException) {
        throw e;
      }
      throw new BaseException(ErrorCode.INTERNAL, "Error creating test model version", e);
    }
  }

  protected void createCommonResources(String storageLocation) throws ApiException {
    CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    CreateRegisteredModel createRm =
        new CreateRegisteredModel()
            .name(MODEL_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .comment(COMMENT);
    RegisteredModelInfo rmInfo = modelOperations.createRegisteredModel(createRm);
    CreateModelVersion createMv =
        new CreateModelVersion()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .comment(MV_COMMENT)
            .source(MV_SOURCE)
            .runId(MV_RUNID);
    modelOperations.createModelVersion(createMv);
    createNonFileModelVersion(rmInfo.getId(), 2L, ModelVersionStatus.PENDING_REGISTRATION, storageLocation);
    createNonFileModelVersion(rmInfo.getId(), 3L, ModelVersionStatus.FAILED_REGISTRATION, storageLocation);
    createNonFileModelVersion(rmInfo.getId(), 4L, ModelVersionStatus.MODEL_VERSION_STATUS_UNKNOWN, storageLocation);
    createNonFileModelVersion(rmInfo.getId(), 5L, ModelVersionStatus.READY, storageLocation);
  }

  @Test
  public void testModelCRUD() throws ApiException {
    // Setup common resources
    createCommonResources("s3://mybucket");
    io.unitycatalog.client.model.ModelVersionInfo fileMv =
        modelOperations.getModelVersion(MODEL_FULL_NAME, 1L);
    io.unitycatalog.client.model.ModelVersionInfo pendingCloudMv =
        modelOperations.getModelVersion(MODEL_FULL_NAME, 2L);
    io.unitycatalog.client.model.ModelVersionInfo failedCloudMv =
        modelOperations.getModelVersion(MODEL_FULL_NAME, 3L);
    io.unitycatalog.client.model.ModelVersionInfo unknownCloudMv =
        modelOperations.getModelVersion(MODEL_FULL_NAME, 4L);
    io.unitycatalog.client.model.ModelVersionInfo readyCloudMv =
        modelOperations.getModelVersion(MODEL_FULL_NAME, 5L);
    assertThat(fileMv.getVersion()).isEqualTo(1L);
    assertThat(fileMv.getStatus().getValue())
        .isEqualTo(ModelVersionStatus.PENDING_REGISTRATION.getValue());
    assertThat(pendingCloudMv.getVersion()).isEqualTo(2L);
    assertThat(pendingCloudMv.getStatus().getValue())
        .isEqualTo(ModelVersionStatus.PENDING_REGISTRATION.getValue());
    assertThat(failedCloudMv.getVersion()).isEqualTo(3L);
    assertThat(failedCloudMv.getStatus().getValue())
        .isEqualTo(ModelVersionStatus.FAILED_REGISTRATION.getValue());
    assertThat(unknownCloudMv.getVersion()).isEqualTo(4L);
    assertThat(unknownCloudMv.getStatus().getValue())
        .isEqualTo(ModelVersionStatus.MODEL_VERSION_STATUS_UNKNOWN.getValue());
    assertThat(readyCloudMv.getVersion()).isEqualTo(5L);
    assertThat(readyCloudMv.getStatus().getValue()).isEqualTo(ModelVersionStatus.READY.getValue());

    // Cannot get credentials for a file based storage location
    GenerateTemporaryModelVersionCredential generateFileCreds =
        new GenerateTemporaryModelVersionCredential()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(1L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () -> temporaryCredentialOperations.generateTemporaryModelVersionCredentials(generateFileCreds))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());

    // Cannot get credentials for a failed status model version
    GenerateTemporaryModelVersionCredential generateCloudFailedCreds =
        new GenerateTemporaryModelVersionCredential()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(3L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                temporaryCredentialOperations.generateTemporaryModelVersionCredentials(
                    generateCloudFailedCreds))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());

    // Cannot get credentials for an unknown status model version
    GenerateTemporaryModelVersionCredential generateCloudUnknownCreds =
        new GenerateTemporaryModelVersionCredential()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(4L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                temporaryCredentialOperations.generateTemporaryModelVersionCredentials(
                    generateCloudUnknownCreds))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());

    // Cannot get read/write credentials for a ready status model version
    GenerateTemporaryModelVersionCredential generateCloudReadyCreds =
        new GenerateTemporaryModelVersionCredential()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(5L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                temporaryCredentialOperations.generateTemporaryModelVersionCredentials(
                    generateCloudReadyCreds))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());

    // Cannot pass in an unknown operation
    GenerateTemporaryModelVersionCredential generateUnknownOperation =
        new GenerateTemporaryModelVersionCredential()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(2L)
            .operation(ModelVersionOperation.UNKNOWN_MODEL_VERSION_OPERATION);

    assertThatThrownBy(
            () ->
                temporaryCredentialOperations.generateTemporaryModelVersionCredentials(
                    generateUnknownOperation))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
  }


  @ParameterizedTest
  @ValueSource(strings = {"s3", "abfs", "gs"})
  public void testGenerateTemporaryPathCredentialsWhereConfIsProvided(String scheme) throws ApiException {
    String url = "";
    // test-bucket0 is configured in server properties
    switch (scheme) {
      case "s3" -> url = "s3://test-bucket0/test";
      case "abfs" -> url = "abfs://test-container@test-bucket0.dfs.core.windows.net/test";
      case "gs" -> url = "gs://test-bucket0/test";
      default -> fail("Invalid scheme");
    }
    // Setup common resources
    createCommonResources(url);
    GenerateTemporaryModelVersionCredential generateCloudReadyCreds =
            new GenerateTemporaryModelVersionCredential()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .modelName(MODEL_NAME)
                    .version(5L)
                    .operation(ModelVersionOperation.READ_MODEL_VERSION);
    TemporaryCredentials temporaryCredentials =
            temporaryCredentialOperations.generateTemporaryModelVersionCredentials(generateCloudReadyCreds);

    switch (scheme) {
      case "s3":
        assertThat(temporaryCredentials.getAwsTempCredentials()).isNotNull();
        assertThat(temporaryCredentials.getAwsTempCredentials().getSessionToken())
                .isEqualTo("test-session-token");
        assertThat(temporaryCredentials.getAwsTempCredentials().getAccessKeyId())
                .isEqualTo("test-access-key-id");
        assertThat(temporaryCredentials.getAwsTempCredentials().getSecretAccessKey())
                .isEqualTo("test-secret-access-key");
        break;
      case "abfs":
        assertThat(temporaryCredentials.getAzureUserDelegationSas()).isNotNull();
        assertThat(temporaryCredentials.getAzureUserDelegationSas().getSasToken())
                .isEqualTo("test-sas-token");
        break;
      case "gs":
        assertThat(temporaryCredentials.getGcpOauthToken()).isNotNull();
        assertThat(temporaryCredentials.getGcpOauthToken().getOauthToken())
                .isEqualTo("test-token");
        break;
      default:
        fail("Invalid scheme");
        break;
    }
  }
}
