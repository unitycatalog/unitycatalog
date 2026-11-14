package io.unitycatalog.server.base.externallocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static io.unitycatalog.server.utils.TestUtils.VOLUME_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

public abstract class BaseExternalLocationCRUDTest extends BaseCRUDTest {
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String NEW_EXTERNAL_LOCATION_NAME = EXTERNAL_LOCATION_NAME + "_new";
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String ANOTHER_CREDENTIAL_NAME = "uc_testcredential_another";
  private static final String NEW_CREDENTIAL_NAME = "uc_testcredential_new";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  protected ExternalLocationOperations externalLocationOperations;
  protected SchemaOperations schemaOperations;
  protected VolumeOperations volumeOperations;
  protected TableOperations tableOperations;

  @TempDir private Path testDir;

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig config);

  protected abstract VolumeOperations createVolumeOperations(ServerConfig config);

  protected abstract TableOperations createTableOperations(ServerConfig config);

  protected CredentialOperations credentialOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected CredentialInfo credentialInfo = null;
  protected CredentialInfo anotherCredentialInfo = null;
  protected Set<String> externalLocationsToDelete = new HashSet<>();

  @SneakyThrows
  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    credentialOperations = createCredentialOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);
    volumeOperations = createVolumeOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);

    // Create catalog and schema
    catalogOperations.createCatalog(new CreateCatalog().name(CATALOG_NAME));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));

    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(CREDENTIAL_NAME)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    credentialInfo = credentialOperations.createCredential(createCredentialRequest);
    createCredentialRequest.setName(ANOTHER_CREDENTIAL_NAME);
    anotherCredentialInfo = credentialOperations.createCredential(createCredentialRequest);
  }

  @SneakyThrows
  @AfterEach
  @Override
  public void tearDown() {
    catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
    for (String externalLocationName : externalLocationsToDelete) {
      externalLocationOperations.deleteExternalLocation(externalLocationName, Optional.of(true));
    }
    externalLocationsToDelete.clear();
    credentialOperations.deleteCredential(CREDENTIAL_NAME, Optional.of(true));
    credentialOperations.deleteCredential(ANOTHER_CREDENTIAL_NAME, Optional.of(true));
    credentialInfo = null;
    anotherCredentialInfo = null;
    super.tearDown();
  }

  private String testUrl() {
    return "file://" + testDir + "/unitycatalog-test";
  }

  private String anotherTestUrl() {
    return testUrl() + "-new";
  }

  protected ExternalLocationInfo create(String name, String url) throws ApiException {
    ExternalLocationInfo externalLocationInfo =
        externalLocationOperations.createExternalLocation(
            new CreateExternalLocation().name(name).url(url).credentialName(CREDENTIAL_NAME));
    externalLocationsToDelete.add(name);
    assertThat(externalLocationInfo.getId()).isNotEmpty();
    assertThat(externalLocationInfo.getName()).isEqualTo(name);
    assertThat(externalLocationInfo.getUrl()).isEqualTo(url);
    assertThat(externalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());
    assertThat(externalLocationInfo.getCredentialName()).isEqualTo(CREDENTIAL_NAME);
    return externalLocationInfo;
  }

  protected void delete(String name, Optional<Boolean> force) throws ApiException {
    externalLocationOperations.deleteExternalLocation(name, force);
    externalLocationsToDelete.remove(name);
    assertApiException(
        () -> externalLocationOperations.getExternalLocation(name),
        ErrorCode.NOT_FOUND,
        "External location not found");
  }

  @Test
  public void testExternalLocationCRUD() throws ApiException {
    // Create an external location
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(testUrl())
            .credentialName("not_exist");
    // Fails as the credential does not exist
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(createExternalLocation),
        ErrorCode.NOT_FOUND,
        "Credential not found: not_exist");

    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .noneMatch(
            externalLocationInfo -> externalLocationInfo.getName().equals(EXTERNAL_LOCATION_NAME));

    ExternalLocationInfo externalLocationInfo = create(EXTERNAL_LOCATION_NAME, testUrl());

    // List external locations
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .contains(externalLocationInfo);

    // Get external location
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .isEqualTo(externalLocationInfo);

    // Update external location
    UpdateExternalLocation updateExternalLocation =
        new UpdateExternalLocation().newName(NEW_EXTERNAL_LOCATION_NAME).url(anotherTestUrl());
    ExternalLocationInfo updatedExternalLocationInfo =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, updateExternalLocation);
    assertThat(updatedExternalLocationInfo.getId()).isEqualTo(externalLocationInfo.getId());
    assertThat(updatedExternalLocationInfo.getName()).isEqualTo(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(updatedExternalLocationInfo.getUrl()).isEqualTo(anotherTestUrl());
    assertThat(updatedExternalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());
    assertThat(updatedExternalLocationInfo.getCredentialName()).isEqualTo(CREDENTIAL_NAME);
    externalLocationsToDelete.remove(EXTERNAL_LOCATION_NAME);
    externalLocationsToDelete.add(NEW_EXTERNAL_LOCATION_NAME);

    // Update external location with new credential
    ExternalLocationInfo updatedExternalLocationInfo2 =
        externalLocationOperations.updateExternalLocation(
            NEW_EXTERNAL_LOCATION_NAME,
            new UpdateExternalLocation().credentialName(ANOTHER_CREDENTIAL_NAME));
    assertThat(updatedExternalLocationInfo2.getId()).isEqualTo(externalLocationInfo.getId());
    assertThat(updatedExternalLocationInfo2.getName()).isEqualTo(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(updatedExternalLocationInfo2.getUrl()).isEqualTo(anotherTestUrl());
    assertThat(updatedExternalLocationInfo2.getCredentialId())
        .isEqualTo(anotherCredentialInfo.getId());
    assertThat(updatedExternalLocationInfo2.getCredentialName()).isEqualTo(ANOTHER_CREDENTIAL_NAME);
  }

  @Test
  public void testExternalLocationRejectsManagedStoragePrefix() throws ApiException {
    ExternalLocationInfo externalLocationInfo = create(EXTERNAL_LOCATION_NAME, testUrl());

    for (String url :
        List.of(testUrl() + "/__unitystorage", testUrl() + "/__unitystorage/tables")) {
      String errorMessage =
          "Input path '" + url + "' contains managed storage prefix __unitystorage.";
      assertApiException(
          () -> create(EXTERNAL_LOCATION_NAME + "_reserved", url),
          ErrorCode.INVALID_ARGUMENT,
          errorMessage);
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  EXTERNAL_LOCATION_NAME,
                  new UpdateExternalLocation().newName(NEW_EXTERNAL_LOCATION_NAME).url(url)),
          ErrorCode.INVALID_ARGUMENT,
          errorMessage);
      assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
          .isEqualTo(externalLocationInfo);
    }

    externalLocationOperations.updateExternalLocation(
        EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(anotherTestUrl()));
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME).getUrl())
        .isEqualTo(anotherTestUrl());
  }

  @Test
  public void testExternalLocationUrlOverlapPrevention() throws ApiException {
    final String errorMessageCreateWithOverlap =
        "Cannot accept an external location that duplicates"
            + " or overlaps with existing external location";
    create(EXTERNAL_LOCATION_NAME, testUrl());

    // Test 1: Duplicate URL should be rejected
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_duplicate", testUrl()),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 2: Duplicate name should be rejected
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME, anotherTestUrl()),
        ErrorCode.EXTERNAL_LOCATION_ALREADY_EXISTS,
        "already exist");

    // Test 3: Child URL should be rejected when parent exists
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_child", testUrl() + "/subpath"),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Update the external location to subdir first
    externalLocationOperations.updateExternalLocation(
        EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(testUrl() + "/subpath/deeper"));

    // Test 4: Parent URL should be rejected when child exists
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_parent", testUrl()),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 5: Trailing slash should be normalized (treated as same URL)
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_slash", testUrl() + "/subpath/deeper/"),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 6: Different buckets should be allowed (no overlap)
    create(EXTERNAL_LOCATION_NAME + "_bucket1", "s3://bucket1/path");
    create(EXTERNAL_LOCATION_NAME + "_bucket2", "s3://bucket2/path");

    // Test 7: Update bucket2 location to use bucket1 with overlapping URL would fail.
    UpdateExternalLocation updateBucket2Location =
        new UpdateExternalLocation().url("s3://bucket1/");
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME + "_bucket2", updateBucket2Location),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 8: sibling paths (same level, different names) are allowed
    create(EXTERNAL_LOCATION_NAME + "_sibling1", testUrl() + "/data1");
    create(EXTERNAL_LOCATION_NAME + "_sibling2", testUrl() + "/data2");
  }

  @Test
  public void testExternalLocationDeletion() throws ApiException, IOException {
    // Test 1: External location with volume - delete without force should fail
    String volumeExternalLocationName = EXTERNAL_LOCATION_NAME + "_volume";
    String volumeUrlRoot = testUrl() + "/volumes";
    create(volumeExternalLocationName, volumeUrlRoot);

    CreateVolumeRequestContent createVolumeRequest =
        new CreateVolumeRequestContent()
            .name(VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(volumeUrlRoot + "/test");
    volumeOperations.createVolume(createVolumeRequest);

    assertApiException(
        () -> delete(volumeExternalLocationName, Optional.empty()),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");

    // Delete with force should succeed
    delete(volumeExternalLocationName, Optional.of(true));

    // Test 2: External location with table - delete without force should fail
    String tableExternalLocationName = EXTERNAL_LOCATION_NAME + "_table";
    String tableUrlRoot = testUrl() + "/tables";
    create(tableExternalLocationName, tableUrlRoot);

    CreateTable createTableRequest =
        new CreateTable()
            .name(TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(tableUrlRoot + "/test")
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeText("integer")
                        .typeName(ColumnTypeName.INT)
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"integer\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .position(0)));
    tableOperations.createTable(createTableRequest);

    assertApiException(
        () -> delete(tableExternalLocationName, Optional.of(false)),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");

    // Delete with force should succeed
    delete(tableExternalLocationName, Optional.of(true));

    // Test 3: External location without entities - delete without force should succeed
    String emptyExternalLocationName = EXTERNAL_LOCATION_NAME + "_empty";
    create(emptyExternalLocationName, testUrl() + "/empty");

    delete(emptyExternalLocationName, Optional.of(false));
  }

  @Test
  public void testExternalLocationSurvivesForceDeletedCredential() throws ApiException {
    String doomedCredentialName = CREDENTIAL_NAME + "_doomed";
    CredentialInfo doomedCredential =
        credentialOperations.createCredential(
            new CreateCredentialRequest()
                .name(doomedCredentialName)
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    externalLocationOperations.createExternalLocation(
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(testUrl())
            .comment("before")
            .credentialName(doomedCredentialName));
    externalLocationsToDelete.add(EXTERNAL_LOCATION_NAME);

    credentialOperations.deleteCredential(doomedCredentialName, Optional.of(true));

    ExternalLocationInfo orphaned =
        externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME);
    assertThat(orphaned.getUrl()).isEqualTo(testUrl());
    assertThat(orphaned.getComment()).isEqualTo("before");
    assertThat(orphaned.getCredentialId()).isEqualTo(doomedCredential.getId());
    assertThat(orphaned.getCredentialName()).isNull();
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .contains(orphaned);

    ExternalLocationInfo commented =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().comment("after"));
    assertThat(commented.getComment()).isEqualTo("after");
    assertThat(commented.getCredentialName()).isNull();

    ExternalLocationInfo repaired =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME,
            new UpdateExternalLocation().credentialName(ANOTHER_CREDENTIAL_NAME));
    assertThat(repaired.getCredentialId()).isEqualTo(anotherCredentialInfo.getId());
    assertThat(repaired.getCredentialName()).isEqualTo(ANOTHER_CREDENTIAL_NAME);
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .isEqualTo(repaired);
  }

  @Test
  public void testPendingCleanupBlocksNormalLocationDeletion() throws ApiException {
    String externalLocationName = EXTERNAL_LOCATION_NAME + "_cleanup";
    String externalLocationRoot = testUrl() + "/cleanup";
    create(externalLocationName, externalLocationRoot);
    List<UUID> taskIds =
        List.of(
            createCleanupTask(externalLocationRoot + "/first"),
            createCleanupTask(externalLocationRoot + "/second"));

    for (UUID taskId : taskIds) {
      assertPendingCleanupDenied(
          () ->
              externalLocationOperations.deleteExternalLocation(
                  externalLocationName, Optional.of(false)));
      int deleted =
          TransactionManager.executeWithTransaction(
              hibernateConfigurator.getSessionFactory(),
              session ->
                  session
                      .createMutationQuery("DELETE FROM StorageCleanupTaskDAO WHERE id = :id")
                      .setParameter("id", taskId)
                      .executeUpdate(),
              "Failed to remove test cleanup task",
              /* readOnly= */ false);
      assertThat(deleted).isEqualTo(1);
    }
    delete(externalLocationName, Optional.of(false));

    // Force deletion remains allowed while cleanup is pending.
    create(externalLocationName, externalLocationRoot);
    createCleanupTask(externalLocationRoot + "/third");
    delete(externalLocationName, Optional.of(true));
  }

  private void assertPendingCleanupDenied(Executable action) {
    assertApiException(
        action, ErrorCode.PERMISSION_DENIED, "Input path overlaps pending storage cleanup");
  }

  private UUID createCleanupTask(String storageLocation) {
    StorageCleanupTaskRepository repository =
        new StorageCleanupTaskRepository(hibernateConfigurator.getSessionFactory());
    return TransactionManager.executeWithTransaction(
        hibernateConfigurator.getSessionFactory(),
        session ->
            repository
                .create(
                    session,
                    ManagedResourceType.TABLE,
                    UUID.randomUUID(),
                    "orders",
                    storageLocation)
                .getId(),
        "Failed to create test cleanup task",
        /* readOnly= */ false);
  }

  /** A drop that waits for a concurrent rename must not delete the renamed external location. */
  @Test
  public void dropRacingARenameDoesNotDeleteTheRenamedExternalLocation() throws Exception {
    UUID id = UUID.fromString(create("drop_race", testUrl()).getId());
    externalLocationsToDelete.remove("drop_race");
    externalLocationsToDelete.add("drop_race_renamed");

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    TestDatabase.setColumn(
                        held, ExternalLocationDAO.class, id, "name", "drop_race_renamed"),
                () -> {
                  externalLocationOperations.deleteExternalLocation("drop_race", Optional.empty());
                  return null;
                }),
        ErrorCode.NOT_FOUND,
        "External location not found");

    assertThat(externalLocationOperations.getExternalLocation("drop_race_renamed").getId())
        .isEqualTo(id.toString());
  }

  /** A rename that waits for a concurrent edit of another column keeps that edit. */
  @Test
  public void renameKeepsAConcurrentCommentEdit() throws Exception {
    UUID id = UUID.fromString(create("edit_race", testUrl()).getId());
    externalLocationsToDelete.remove("edit_race");
    externalLocationsToDelete.add("edited");

    runBlockedByHeldWrite(
        held ->
            TestDatabase.setColumn(held, ExternalLocationDAO.class, id, "comment", "held comment"),
        () ->
            externalLocationOperations.updateExternalLocation(
                "edit_race", new UpdateExternalLocation().newName("edited")));

    ExternalLocationInfo renamed = externalLocationOperations.getExternalLocation("edited");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo("held comment");
  }

  /** A comment-only update that waits for a concurrent credential change names that credential. */
  @Test
  public void commentUpdateReturnsTheConcurrentlyChangedCredentialName() throws Exception {
    UUID id = UUID.fromString(create("cred_race", testUrl()).getId());
    UUID anotherCredentialId = UUID.fromString(anotherCredentialInfo.getId());

    ExternalLocationInfo updated =
        runBlockedByHeldWrite(
            held ->
                TestDatabase.setColumn(
                    held, ExternalLocationDAO.class, id, "credentialId", anotherCredentialId),
            () ->
                externalLocationOperations.updateExternalLocation(
                    "cred_race", new UpdateExternalLocation().comment("patched")));

    assertThat(updated.getCredentialId()).isEqualTo(anotherCredentialInfo.getId());
    assertThat(updated.getCredentialName()).isEqualTo(ANOTHER_CREDENTIAL_NAME);
  }

  /** A comment-only update that waits for a credential created after it started names that one. */
  @Test
  public void commentUpdateReturnsTheNameOfAConcurrentlyCreatedCredential() throws Exception {
    UUID id = UUID.fromString(create("new_cred_race", testUrl()).getId());
    CredentialDAO newCredential =
        CredentialDAO.from(
            new io.unitycatalog.server.model.CreateCredentialRequest()
                .name(NEW_CREDENTIAL_NAME)
                .purpose(io.unitycatalog.server.model.CredentialPurpose.STORAGE)
                .awsIamRole(
                    new io.unitycatalog.server.model.AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)),
            "test-user");

    ExternalLocationInfo updated =
        runBlockedByHeldWrite(
            held -> {
              held.persist(newCredential);
              TestDatabase.setColumn(
                  held, ExternalLocationDAO.class, id, "credentialId", newCredential.getId());
            },
            () ->
                externalLocationOperations.updateExternalLocation(
                    "new_cred_race", new UpdateExternalLocation().comment("patched")));

    assertThat(updated.getCredentialId()).isEqualTo(newCredential.getId().toString());
    assertThat(updated.getCredentialName()).isEqualTo(NEW_CREDENTIAL_NAME);
    credentialOperations.deleteCredential(NEW_CREDENTIAL_NAME, Optional.of(true));
  }

  /** An update that waits for a concurrent rename of the location misses it, changing nothing. */
  @Test
  public void updateRacingARenameIsNotFound() throws Exception {
    UUID id = UUID.fromString(create("update_race", testUrl()).getId());
    externalLocationsToDelete.remove("update_race");
    externalLocationsToDelete.add("update_race_renamed");

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    TestDatabase.setColumn(
                        held, ExternalLocationDAO.class, id, "name", "update_race_renamed"),
                () ->
                    externalLocationOperations.updateExternalLocation(
                        "update_race", new UpdateExternalLocation().comment("lost"))),
        ErrorCode.NOT_FOUND,
        "External location not found");

    ExternalLocationInfo renamed =
        externalLocationOperations.getExternalLocation("update_race_renamed");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isNull();
  }

  /** A delete that waits behind a write sees a table that write created under the location. */
  @Test
  public void deleteWaitingBehindAWriteSeesATableItCreated() throws Exception {
    String url = testUrl() + "/race";
    UUID id = UUID.fromString(create("delete_race", url).getId());
    UUID schemaId =
        UUID.fromString(schemaOperations.getSchema(CATALOG_NAME + "." + SCHEMA_NAME).getSchemaId());

    assertApiException(
        () ->
            runBlockedByHeldWrite(
                held -> {
                  // Holds the location row, and creates a table under it in the same commit.
                  TestDatabase.setColumn(held, ExternalLocationDAO.class, id, "comment", "held");
                  held.persist(
                      TableInfoDAO.builder()
                          .id(UUID.randomUUID())
                          .name("race_table")
                          .schemaId(schemaId)
                          .type(TableType.EXTERNAL.getValue())
                          .dataSourceFormat(DataSourceFormat.DELTA.getValue())
                          .url(url + "/t")
                          .columnCount(0)
                          .createdAt(new Date())
                          .build());
                },
                () -> {
                  delete("delete_race", Optional.of(false));
                  return null;
                }),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");

    assertThat(externalLocationOperations.getExternalLocation("delete_race").getId())
        .isEqualTo(id.toString());
  }
}
