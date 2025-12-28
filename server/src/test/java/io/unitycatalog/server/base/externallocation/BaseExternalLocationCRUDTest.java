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
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseExternalLocationCRUDTest extends BaseCRUDTest {
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String NEW_EXTERNAL_LOCATION_NAME = EXTERNAL_LOCATION_NAME + "_new";
  private static final String URL = "s3://unitycatalog-test";
  private static final String NEW_URL = "s3://unitycatalog-test-new";
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  protected ExternalLocationOperations externalLocationOperations;
  protected SchemaOperations schemaOperations;
  protected VolumeOperations volumeOperations;
  protected TableOperations tableOperations;

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig config);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig config);

  protected abstract VolumeOperations createVolumeOperations(ServerConfig config);

  protected abstract TableOperations createTableOperations(ServerConfig config);

  protected CredentialOperations credentialOperations;

  protected abstract CredentialOperations createCredentialOperations(ServerConfig config);

  protected CredentialInfo credentialInfo = null;
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
    credentialOperations.deleteCredential(credentialInfo.getName());
    credentialInfo = null;
    super.tearDown();
  }

  protected ExternalLocationInfo create(String name, String url) throws ApiException {
    ExternalLocationInfo externalLocationInfo =
        externalLocationOperations.createExternalLocation(
            new CreateExternalLocation().name(name).url(url).credentialName(CREDENTIAL_NAME));
    externalLocationsToDelete.add(name);
    assertThat(externalLocationInfo.getName()).isEqualTo(name);
    assertThat(externalLocationInfo.getUrl()).isEqualTo(url);
    assertThat(externalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());
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
            .url(URL)
            .credentialName("not_exist");
    // Fails as the credential does not exist
    assertApiException(
        () -> externalLocationOperations.createExternalLocation(createExternalLocation),
        ErrorCode.NOT_FOUND,
        "Credential not found: not_exist");

    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .noneMatch(
            externalLocationInfo -> externalLocationInfo.getName().equals(EXTERNAL_LOCATION_NAME));

    ExternalLocationInfo externalLocationInfo = create(EXTERNAL_LOCATION_NAME, URL);

    // List external locations
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .contains(externalLocationInfo);

    // Get external location
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME))
        .isEqualTo(externalLocationInfo);

    // Update external location
    UpdateExternalLocation updateExternalLocation =
        new UpdateExternalLocation().newName(NEW_EXTERNAL_LOCATION_NAME).url(NEW_URL);
    ExternalLocationInfo updatedExternalLocationInfo =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, updateExternalLocation);
    assertThat(updatedExternalLocationInfo.getName()).isEqualTo(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(updatedExternalLocationInfo.getUrl()).isEqualTo(NEW_URL);
    assertThat(updatedExternalLocationInfo.getCredentialId()).isEqualTo(credentialInfo.getId());
    externalLocationsToDelete.remove(EXTERNAL_LOCATION_NAME);
    externalLocationsToDelete.add(NEW_EXTERNAL_LOCATION_NAME);
  }

  @Test
  public void testExternalLocationUrlOverlapPrevention() throws ApiException {
    final String errorMessageCreateWithOverlap =
        "Cannot accept an external location that duplicates"
            + " or overlaps with existing external location";
    create(EXTERNAL_LOCATION_NAME, URL);

    // Test 1: Duplicate URL should be rejected
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_duplicate", URL),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 2: Duplicate name should be rejected
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME, NEW_URL), ErrorCode.ALREADY_EXISTS, "already exist");

    // Test 3: Child URL should be rejected when parent exists
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_child", URL + "/subpath"),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Update the external location to subdir first
    externalLocationOperations.updateExternalLocation(
        EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(URL + "/subpath/deeper"));

    // Test 4: Parent URL should be rejected when child exists
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_parent", URL),
        ErrorCode.INVALID_ARGUMENT,
        errorMessageCreateWithOverlap);

    // Test 5: Trailing slash should be normalized (treated as same URL)
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME + "_slash", URL + "/subpath/deeper/"),
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
    create(EXTERNAL_LOCATION_NAME + "_sibling1", URL + "/data1");
    create(EXTERNAL_LOCATION_NAME + "_sibling2", URL + "/data2");
  }

  @Test
  public void testExternalLocationDeletion() throws ApiException, IOException {
    // Test 1: External location with volume - delete without force should fail
    create(EXTERNAL_LOCATION_NAME + "_volume", URL + "/volumes");

    CreateVolumeRequestContent createVolumeRequest =
        new CreateVolumeRequestContent()
            .name(VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(URL + "/volumes/test");
    volumeOperations.createVolume(createVolumeRequest);

    assertApiException(
        () -> delete(EXTERNAL_LOCATION_NAME + "_volume", Optional.empty()),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");

    // Delete with force should succeed
    delete(EXTERNAL_LOCATION_NAME + "_volume", Optional.of(true));

    // Test 2: External location with table - delete without force should fail
    create(EXTERNAL_LOCATION_NAME + "_table", URL + "/tables");

    CreateTable createTableRequest =
        new CreateTable()
            .name(TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(URL + "/tables/test_table")
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeText("integer")
                        .typeName(ColumnTypeName.INT)
                        .typeJson("{\"type\": \"integer\"}")
                        .position(0)));
    tableOperations.createTable(createTableRequest);

    assertApiException(
        () -> delete(EXTERNAL_LOCATION_NAME + "_table", Optional.of(false)),
        ErrorCode.INVALID_ARGUMENT,
        "External location still used by");

    // Delete with force should succeed
    delete(EXTERNAL_LOCATION_NAME + "_table", Optional.of(true));

    // Test 3: External location without entities - delete without force should succeed
    create(EXTERNAL_LOCATION_NAME + "_empty", URL + "/empty");

    delete(EXTERNAL_LOCATION_NAME + "_empty", Optional.of(false));
  }
}
