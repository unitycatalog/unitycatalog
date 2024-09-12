package io.unitycatalog.server.base.credentials;

import static io.unitycatalog.server.utils.TestUtils.*;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ModelVersionInfo;
import io.unitycatalog.server.model.ModelVersionStatus;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTemporaryModelVersionCredentialsTest extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected ModelOperations modelOperations;
  protected TemporaryModelVersionCredentialsOperations credentialsOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract ModelOperations createModelOperations(ServerConfig serverConfig);

  protected abstract TemporaryModelVersionCredentialsOperations
      createTemporaruModelVersionCredentialsOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
    credentialsOperations = createTemporaruModelVersionCredentialsOperations(serverConfig);
  }

  protected void createNonFileModelVersion(
      String modelId, long version, ModelVersionStatus status) {
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
            .storageLocation("s3://mybucket")
            .comment(COMMENT)
            .createdAt(createTime)
            .updatedAt(createTime);
    Transaction tx;
    try (Session session = HibernateUtils.getSessionFactory().openSession()) {
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

  protected void createCommonResources() throws ApiException {
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
    createNonFileModelVersion(rmInfo.getId(), 2L, ModelVersionStatus.PENDING_REGISTRATION);
    createNonFileModelVersion(rmInfo.getId(), 3L, ModelVersionStatus.FAILED_REGISTRATION);
    createNonFileModelVersion(rmInfo.getId(), 4L, ModelVersionStatus.MODEL_VERSION_STATUS_UNKNOWN);
    createNonFileModelVersion(rmInfo.getId(), 5L, ModelVersionStatus.READY);
  }

  @Test
  public void testModelCRUD() throws ApiException {
    // Setup common resources
    createCommonResources();
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
    GenerateTemporaryModelVersionCredentials generateFileCreds =
        new GenerateTemporaryModelVersionCredentials()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(1L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () -> credentialsOperations.generateTemporaryModelVersionCredentials(generateFileCreds))
        .isInstanceOf(Exception.class);
    try {
      credentialsOperations.generateTemporaryModelVersionCredentials(generateFileCreds);
    } catch (ApiException e) {
      assertThat(e.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    }

    // Cannot get credentials for a failed status model version
    GenerateTemporaryModelVersionCredentials generateCloudFailedCreds =
        new GenerateTemporaryModelVersionCredentials()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(3L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                credentialsOperations.generateTemporaryModelVersionCredentials(
                    generateCloudFailedCreds))
        .isInstanceOf(Exception.class);
    try {
      credentialsOperations.generateTemporaryModelVersionCredentials(generateCloudFailedCreds);
    } catch (ApiException e) {
      assertThat(e.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    }

    // Cannot get credentials for an unknown status model version
    GenerateTemporaryModelVersionCredentials generateCloudUnknownCreds =
        new GenerateTemporaryModelVersionCredentials()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(4L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                credentialsOperations.generateTemporaryModelVersionCredentials(
                    generateCloudUnknownCreds))
        .isInstanceOf(Exception.class);
    try {
      credentialsOperations.generateTemporaryModelVersionCredentials(generateCloudUnknownCreds);
    } catch (ApiException e) {
      assertThat(e.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    }

    // Cannot get read/write credentials for a ready status model version
    GenerateTemporaryModelVersionCredentials generateCloudReadyCreds =
        new GenerateTemporaryModelVersionCredentials()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(5L)
            .operation(ModelVersionOperation.READ_WRITE_MODEL_VERSION);

    assertThatThrownBy(
            () ->
                credentialsOperations.generateTemporaryModelVersionCredentials(
                    generateCloudReadyCreds))
        .isInstanceOf(Exception.class);
    try {
      credentialsOperations.generateTemporaryModelVersionCredentials(generateCloudReadyCreds);
    } catch (ApiException e) {
      assertThat(e.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    }

    // Cannot pass in an unknown operation
    GenerateTemporaryModelVersionCredentials generateUnknownOperation =
        new GenerateTemporaryModelVersionCredentials()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .modelName(MODEL_NAME)
            .version(2L)
            .operation(ModelVersionOperation.UNKNOWN_MODEL_VERSION_OPERATION);

    assertThatThrownBy(
            () ->
                credentialsOperations.generateTemporaryModelVersionCredentials(
                    generateUnknownOperation))
        .isInstanceOf(Exception.class);
    try {
      credentialsOperations.generateTemporaryModelVersionCredentials(generateUnknownOperation);
    } catch (ApiException e) {
      assertThat(e.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    }
  }
}
