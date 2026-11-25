package io.unitycatalog.server.base.externallocation;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static io.unitycatalog.server.utils.TestUtils.VOLUME_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.client.model.VolumeInfo;
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
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
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
  protected static final String COMPAT_CREDENTIAL_NAME = "uc_testcredential_compat";
  protected static final String ANOTHER_COMPAT_CREDENTIAL_NAME = "uc_testcredential_compat_another";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";
  // A bucket the server assumes its own role for, so its STS session policy is checked.
  private static final String ROLE_BUCKET = "s3://uc-role-bucket";
  private static final String ROOTED_CATALOG_NAME = "uc_testrootedcatalog";
  private static final String ROOTED_SCHEMA_NAME = "uc_testrootedschema";
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

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    enableS3CompatibleStorage();
    serverProperties.setProperty("s3.bucketPath.0", ROLE_BUCKET);
    serverProperties.setProperty("s3.region.0", "us-west-2");
    serverProperties.setProperty("s3.awsRoleArn.0", DUMMY_ROLE_ARN);
  }

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
  public void testS3CompatibleLocations() throws ApiException {
    String endpointA = "http://storage-a.invalid:9000";
    String endpointB = "http://storage-b.invalid:9000";
    createCompatCredential(COMPAT_CREDENTIAL_NAME, endpointA);
    createCompatCredential(ANOTHER_COMPAT_CREDENTIAL_NAME, endpointB);

    // Such a credential reaches storage only through the S3 API, at an s3:// path with a bucket.
    for (String url :
        List.of(
            "gs://shared-bucket/data",
            "s3:/shared-bucket/data",
            "s3:///shared-bucket/data",
            "s3://?shared-bucket",
            "s3://#shared-bucket",
            "s3://:9000/data",
            "s3://:/data",
            "s3://@/data",
            "s3://user@shared-bucket/data",
            "s3://shared-bucket:9000/data")) {
      assertApiException(
          () -> createLocation("el_other", url, COMPAT_CREDENTIAL_NAME),
          ErrorCode.INVALID_ARGUMENT,
          "its external locations must be s3:// paths");
    }

    // A bucket name S3 allows that a URI host does not, such as one with an underscore.
    createLocation("el_underscore", "s3://my_bucket/data", COMPAT_CREDENTIAL_NAME);
    delete("el_underscore", Optional.empty());

    // One bucket name on two storage systems, under locations that do not overlap.
    createLocation("el_a", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME);
    createLocation("el_b", "s3://shared-bucket/b", ANOTHER_COMPAT_CREDENTIAL_NAME);
    assertApiException(
        () -> createLocation("el_bucket", "s3://shared-bucket", COMPAT_CREDENTIAL_NAME),
        ErrorCode.INVALID_ARGUMENT,
        "overlaps with existing external location");
    // Updates keep the same rule: a new URL, or a new credential.
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                "el_a", new UpdateExternalLocation().url("gs://shared-bucket/a")),
        ErrorCode.INVALID_ARGUMENT,
        "its external locations must be s3:// paths");

    // A credential can change type while locations use it, as long as an s3_compatible credential
    // keeps only s3:// locations.
    for (UpdateCredentialRequest update :
        List.of(
            new UpdateCredentialRequest()
                .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)),
            new UpdateCredentialRequest().s3Compatible(keyPair(endpointA)))) {
      credentialOperations.updateCredential(COMPAT_CREDENTIAL_NAME, update);
    }
    create(EXTERNAL_LOCATION_NAME, testUrl());
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME,
                new UpdateExternalLocation().credentialName(COMPAT_CREDENTIAL_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        "its external locations must be s3:// paths");
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                CREDENTIAL_NAME, new UpdateCredentialRequest().s3Compatible(keyPair(endpointA))),
        ErrorCode.INVALID_ARGUMENT,
        "Cannot make credential '" + CREDENTIAL_NAME + "' an s3_compatible");

    for (String name : List.of("el_a", "el_b")) {
      delete(name, Optional.empty());
    }
    for (String name : List.of(COMPAT_CREDENTIAL_NAME, ANOTHER_COMPAT_CREDENTIAL_NAME)) {
      credentialOperations.deleteCredential(name, Optional.empty());
    }
  }

  @Test
  public void testUrlUpToTheColumnLimit() throws ApiException {
    String longestUrl = testUrl() + "/" + "u".repeat(255 - testUrl().length() - 1);
    assertThat(longestUrl).hasSize(255);
    assertApiException(
        () -> create(EXTERNAL_LOCATION_NAME, longestUrl + "u"),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 256 characters, and the limit is 255.");
    create(EXTERNAL_LOCATION_NAME, testUrl());
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(longestUrl + "u")),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 256 characters, and the limit is 255.");
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME,
                new UpdateExternalLocation()
                    .url(longestUrl + "u")
                    .credentialName(ANOTHER_CREDENTIAL_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 256 characters, and the limit is 255.");
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(longestUrl))
                .getUrl())
        .isEqualTo(longestUrl);
  }

  @Test
  public void testCredentialChangeChecksTheUrlAgainstTheNewCredential() throws ApiException {
    createCompatCredential(COMPAT_CREDENTIAL_NAME, "http://storage-a.invalid:9000");
    // The AWS STS session policy repeats the path five times and escapes each * to ${*}, so this
    // URL's policy is over 2,048 characters, a limit S3-compatible storage doesn't have.
    String url = "s3://bucket/" + "*".repeat(100);
    createLocation(EXTERNAL_LOCATION_NAME, url, COMPAT_CREDENTIAL_NAME);

    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME,
                new UpdateExternalLocation().credentialName(CREDENTIAL_NAME)),
        ErrorCode.INVALID_ARGUMENT,
        "its AWS STS session policy is");
    assertApiException(
        () ->
            createLocation(
                NEW_EXTERNAL_LOCATION_NAME, url.replace("bucket", "other-bucket"), CREDENTIAL_NAME),
        ErrorCode.INVALID_ARGUMENT,
        "its AWS STS session policy is");
    assertThat(
            externalLocationOperations
                .getExternalLocation(EXTERNAL_LOCATION_NAME)
                .getCredentialName())
        .isEqualTo(COMPAT_CREDENTIAL_NAME);
  }

  @Test
  public void testUrlLengthIsCountedTheWayTheDatabaseCountsIt() throws ApiException {
    // An emoji is one character in PostgreSQL and MySQL, which count code points, and two in H2,
    // which counts UTF-16 code units.
    String emoji = new String(Character.toChars(0x1F600));
    String prefix = testUrl() + "/";
    String url = prefix + emoji.repeat(255 - prefix.length());
    if (SupportedDatabase.selected() == SupportedDatabase.H2) {
      assertApiException(
          () -> create(EXTERNAL_LOCATION_NAME, url),
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Storage location is too long: it is %d characters, and the limit is 255.",
              url.length()));
    } else {
      assertThat(create(EXTERNAL_LOCATION_NAME, url).getUrl()).isEqualTo(url);
      assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME).getUrl())
          .isEqualTo(url);
    }
  }

  @Test
  public void testLocationWithAUrlFromAnEarlierReleaseCanStillChange() throws ApiException {
    // An earlier release stored URLs that no longer parse, such as one with a query. Changing
    // anything but the URL still works, as does moving it to a credential other than
    // s3_compatible.
    String legacyUrl = "file:///tmp/legacy?x=1";
    create(EXTERNAL_LOCATION_NAME, testUrl());
    setStoredUrl(EXTERNAL_LOCATION_NAME, legacyUrl);

    ExternalLocationInfo renamed =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME,
            new UpdateExternalLocation().newName(NEW_EXTERNAL_LOCATION_NAME));
    externalLocationsToDelete.remove(EXTERNAL_LOCATION_NAME);
    externalLocationsToDelete.add(NEW_EXTERNAL_LOCATION_NAME);
    assertThat(renamed.getUrl()).isEqualTo(legacyUrl);
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    NEW_EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().comment("comment"))
                .getComment())
        .isEqualTo("comment");
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    NEW_EXTERNAL_LOCATION_NAME,
                    new UpdateExternalLocation().credentialName(ANOTHER_CREDENTIAL_NAME))
                .getCredentialName())
        .isEqualTo(ANOTHER_CREDENTIAL_NAME);
    // It is still not an s3:// path, so its credential cannot become s3_compatible.
    assertApiException(
        () ->
            credentialOperations.updateCredential(
                ANOTHER_CREDENTIAL_NAME,
                new UpdateCredentialRequest().s3Compatible(keyPair("http://s3:9000"))),
        ErrorCode.INVALID_ARGUMENT,
        "Cannot make credential '" + ANOTHER_CREDENTIAL_NAME + "' an s3_compatible");

    // Nor is a stored s3:// URL that no longer parses. The credential's error leaves it out, as
    // managing a credential does not grant reading its locations, and moving the location to an
    // s3_compatible credential is refused rather than failing in the parser.
    createCompatCredential(COMPAT_CREDENTIAL_NAME, "http://s3:9000");
    for (String storedUrl : List.of("s3://secret-bucket/a b", "S3://secret-bucket/x", "s3:///")) {
      setStoredUrl(NEW_EXTERNAL_LOCATION_NAME, storedUrl);
      ApiException credentialChange =
          assertThrows(
              ApiException.class,
              () ->
                  credentialOperations.updateCredential(
                      ANOTHER_CREDENTIAL_NAME,
                      new UpdateCredentialRequest().s3Compatible(keyPair("http://s3:9000"))));
      assertThat(credentialChange.getCode()).as(storedUrl).isEqualTo(400);
      assertThat(credentialChange.getResponseBody())
          .as(storedUrl)
          .contains("Cannot make credential '" + ANOTHER_CREDENTIAL_NAME + "' an s3_compatible")
          .doesNotContain("secret-bucket");
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  NEW_EXTERNAL_LOCATION_NAME,
                  new UpdateExternalLocation().credentialName(COMPAT_CREDENTIAL_NAME)),
          ErrorCode.INVALID_ARGUMENT,
          "its external locations must be s3:// paths");
    }
    credentialOperations.deleteCredential(COMPAT_CREDENTIAL_NAME, Optional.empty());
  }

  /**
   * A URL change or delete without force is refused, as the location can't be checked for use. A
   * forced one only logs, so it still works.
   */
  @Test
  public void testForcedChangesWorkWhenTheStoredUrlDoesNotParse() throws ApiException {
    create(EXTERNAL_LOCATION_NAME, testUrl());
    String notChecked =
        "The stored URL of external location '"
            + EXTERNAL_LOCATION_NAME
            + "' cannot be checked for use, as it does not parse. Set force to true to skip this"
            + " check.";
    for (String storedUrl : List.of("s3:///", "s3://secret-bucket/a b")) {
      setStoredUrl(EXTERNAL_LOCATION_NAME, storedUrl);
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(anotherTestUrl())),
          ErrorCode.INVALID_ARGUMENT,
          notChecked);
      assertApiException(
          () ->
              externalLocationOperations.deleteExternalLocation(
                  EXTERNAL_LOCATION_NAME, Optional.of(false)),
          ErrorCode.INVALID_ARGUMENT,
          notChecked);
    }

    setStoredUrl(EXTERNAL_LOCATION_NAME, "s3:///");
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME,
                    new UpdateExternalLocation().url(anotherTestUrl()).force(true))
                .getUrl())
        .isEqualTo(anotherTestUrl());

    setStoredUrl(EXTERNAL_LOCATION_NAME, "s3:///");
    delete(EXTERNAL_LOCATION_NAME, Optional.of(true));
  }

  /** Stores {@code url} for a location as an earlier release might have, without validating it. */
  private void setStoredUrl(String locationName, String url) {
    TransactionManager.executeWithTransaction(
        hibernateConfigurator.getSessionFactory(),
        session ->
            session
                .createMutationQuery("UPDATE ExternalLocationDAO SET url = :url WHERE name = :name")
                .setParameter("url", url)
                .setParameter("name", locationName)
                .executeUpdate(),
        "Failed to store a URL",
        /* readOnly= */ false);
  }

  protected void createCompatCredential(String name, String s3Endpoint) throws ApiException {
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .s3Compatible(keyPair(s3Endpoint)));
  }

  protected void createLocation(String name, String url, String credentialName)
      throws ApiException {
    externalLocationOperations.createExternalLocation(
        new CreateExternalLocation().name(name).url(url).credentialName(credentialName));
    externalLocationsToDelete.add(name);
  }

  protected static S3CompatibleRequest keyPair(String s3Endpoint) {
    return new S3CompatibleRequest()
        .accessKeyId("key-id-1")
        .secretAccessKey("key-value-1")
        .s3Endpoint(s3Endpoint);
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
  public void testUrlChangeWhileInUse() throws ApiException {
    String root = testUrl() + "/in-use";
    create(EXTERNAL_LOCATION_NAME, root);
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name(VOLUME_NAME)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(root + "/volume"));
    String usedByVolume =
        "External location still used by volume '"
            + volume.getVolumeId()
            + "'. Set force to true to skip this check.";

    // A new URL that does not cover the old one would leave the volume without credentials.
    for (String url : List.of(anotherTestUrl(), root + "/other", testUrl() + "/in")) {
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(url).force(false)),
          ErrorCode.INVALID_ARGUMENT,
          usedByVolume);
    }
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME).getUrl())
        .isEqualTo(root);

    // A refused URL change also rolls back a rename sent with it.
    String newName = EXTERNAL_LOCATION_NAME + "_renamed";
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME,
                new UpdateExternalLocation().newName(newName).url(anotherTestUrl())),
        ErrorCode.INVALID_ARGUMENT,
        usedByVolume);
    assertThat(externalLocationOperations.getExternalLocation(EXTERNAL_LOCATION_NAME).getUrl())
        .isEqualTo(root);
    assertApiException(
        () -> externalLocationOperations.getExternalLocation(newName),
        ErrorCode.NOT_FOUND,
        "External location not found: " + newName);

    // Resending the current URL is not a change.
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(root).force(false))
                .getUrl())
        .isEqualTo(root);

    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().comment("in use"))
                .getComment())
        .isEqualTo("in use");

    // A parent URL still covers the volume.
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(testUrl()))
                .getUrl())
        .isEqualTo(testUrl());

    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME,
                    new UpdateExternalLocation().url(anotherTestUrl()).force(true))
                .getUrl())
        .isEqualTo(anotherTestUrl());
  }

  @Test
  public void testCatalogAndSchemaManagedStorageKeepLocationInUse() throws ApiException {
    String catalogLocationName = EXTERNAL_LOCATION_NAME + "_catalog";
    String catalogRoot = testUrl() + "/catalog-root";
    create(catalogLocationName, catalogRoot);
    catalogOperations.createCatalog(
        new CreateCatalog().name(ROOTED_CATALOG_NAME).storageRoot(catalogRoot));

    String schemaLocationName = EXTERNAL_LOCATION_NAME + "_schema";
    String schemaRoot = testUrl() + "/schema-root";
    create(schemaLocationName, schemaRoot);
    schemaOperations.createSchema(
        new CreateSchema()
            .catalogName(CATALOG_NAME)
            .name(ROOTED_SCHEMA_NAME)
            .storageRoot(schemaRoot));

    for (List<String> locationAndUser :
        List.of(
            List.of(catalogLocationName, "a catalog"), List.of(schemaLocationName, "a schema"))) {
      String locationName = locationAndUser.get(0);
      String usedBy =
          "External location still used by "
              + locationAndUser.get(1)
              + ". Set force to true to skip this check.";
      assertApiException(
          () -> delete(locationName, Optional.of(false)), ErrorCode.INVALID_ARGUMENT, usedBy);
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  locationName, new UpdateExternalLocation().url(anotherTestUrl())),
          ErrorCode.INVALID_ARGUMENT,
          usedBy);
    }

    schemaOperations.deleteSchema(CATALOG_NAME + "." + ROOTED_SCHEMA_NAME, Optional.of(true));
    catalogOperations.deleteCatalog(ROOTED_CATALOG_NAME, Optional.of(true));
    for (String locationName : List.of(catalogLocationName, schemaLocationName)) {
      String newUrl = anotherTestUrl() + "/" + locationName;
      assertThat(
              externalLocationOperations
                  .updateExternalLocation(locationName, new UpdateExternalLocation().url(newUrl))
                  .getUrl())
          .isEqualTo(newUrl);
      delete(locationName, Optional.of(false));
    }
  }

  /**
   * An uncommitted staging table left behind by a dropped schema doesn't keep the location in use,
   * since orphaned staging rows are never removed.
   */
  @Test
  public void testOrphanedStagingTableDoesNotKeepLocationInUse() throws ApiException {
    String locationName = EXTERNAL_LOCATION_NAME + "_staging";
    String schemaRoot = testUrl() + "/staging-root";
    create(locationName, schemaRoot);
    schemaOperations.createSchema(
        new CreateSchema()
            .catalogName(CATALOG_NAME)
            .name(ROOTED_SCHEMA_NAME)
            .storageRoot(schemaRoot));
    StagingTableInfo staging =
        new TablesApi(TestUtils.createApiClient(serverConfig))
            .createStagingTable(
                new CreateStagingTable()
                    .catalogName(CATALOG_NAME)
                    .schemaName(ROOTED_SCHEMA_NAME)
                    .name("orphaned"));
    assertThat(staging.getStagingLocation()).startsWith(schemaRoot + "/");
    schemaOperations.deleteSchema(CATALOG_NAME + "." + ROOTED_SCHEMA_NAME, Optional.of(true));

    delete(locationName, Optional.of(false));
  }

  /**
   * A location under an external location is checked with that location's storage credential. Once
   * the credential is force-deleted, a vend fails on it, so a URL change is checked only against
   * the column, even on a bucket the server has its own role for.
   */
  @Test
  public void testLocationsAreCheckedWithTheCredentialOfTheirExternalLocation()
      throws ApiException {
    String doomedCredentialName = CREDENTIAL_NAME + "_doomed";
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(doomedCredentialName)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    // The server has no configuration for this bucket, so only the credential limits its paths.
    String url = "s3://uc-unconfigured-bucket/external";
    createLocation(EXTERNAL_LOCATION_NAME, url, doomedCredentialName);
    // The AWS STS session policy repeats the path five times and escapes each * to ${*}, so a path
    // with 100 of them has a policy over 2,048 characters.
    String policyExpanding = "/" + "*".repeat(100);

    volumeOperations.createVolume(externalVolume(VOLUME_NAME, url + "/short"));
    assertApiException(
        () ->
            volumeOperations.createVolume(externalVolume(VOLUME_NAME + "2", url + policyExpanding)),
        ErrorCode.INVALID_ARGUMENT,
        "its AWS STS session policy is");

    credentialOperations.deleteCredential(doomedCredentialName, Optional.of(true));
    String roleBucketUrl = ROLE_BUCKET + "/external" + policyExpanding;
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    EXTERNAL_LOCATION_NAME,
                    new UpdateExternalLocation().url(roleBucketUrl).force(true))
                .getUrl())
        .isEqualTo(roleBucketUrl);
    String tooLong = ROLE_BUCKET + "/" + "u".repeat(256 - ROLE_BUCKET.length() - 1);
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(tooLong).force(true)),
        ErrorCode.INVALID_ARGUMENT,
        "Storage location is too long: it is 256 characters, and the limit is 255.");
  }

  private static CreateVolumeRequestContent externalVolume(String name, String location) {
    return new CreateVolumeRequestContent()
        .name(name)
        .catalogName(CATALOG_NAME)
        .schemaName(SCHEMA_NAME)
        .volumeType(VolumeType.EXTERNAL)
        .storageLocation(location);
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
    // Its URL can change too: with no credential, no credential type restricts the URL.
    ExternalLocationInfo moved =
        externalLocationOperations.updateExternalLocation(
            EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(testUrl() + "/moved"));
    assertThat(moved.getUrl()).isEqualTo(testUrl() + "/moved");
    assertThat(moved.getCredentialName()).isNull();

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
      assertPendingCleanupRefused(
          () ->
              externalLocationOperations.deleteExternalLocation(
                  externalLocationName, Optional.of(false)));
      assertPendingCleanupRefused(
          () ->
              externalLocationOperations.updateExternalLocation(
                  externalLocationName, new UpdateExternalLocation().url(anotherTestUrl())));
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

    // Forced changes remain allowed while cleanup is pending.
    create(externalLocationName, externalLocationRoot);
    createCleanupTask(externalLocationRoot + "/third");
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    externalLocationName,
                    new UpdateExternalLocation().url(anotherTestUrl()).force(true))
                .getUrl())
        .isEqualTo(anotherTestUrl());
    createCleanupTask(anotherTestUrl() + "/fourth");
    assertPendingCleanupRefused(
        () ->
            externalLocationOperations.deleteExternalLocation(
                externalLocationName, Optional.of(false)));
    delete(externalLocationName, Optional.of(true));
  }

  @Test
  public void testPendingCleanupIsReportedBeforeAStorageUser() throws ApiException {
    String root = testUrl() + "/cleanup-and-volume";
    create(EXTERNAL_LOCATION_NAME, root);
    volumeOperations.createVolume(
        new CreateVolumeRequestContent()
            .name(VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(root + "/volume"));
    createCleanupTask(root + "/cleanup");

    assertPendingCleanupRefused(
        () ->
            externalLocationOperations.deleteExternalLocation(
                EXTERNAL_LOCATION_NAME, Optional.of(false)));
    assertPendingCleanupRefused(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().url(anotherTestUrl())));
  }

  private void assertPendingCleanupRefused(Executable action) {
    assertApiException(
        action,
        ErrorCode.FAILED_PRECONDITION,
        "Input path overlaps pending storage cleanup. Set force to true to skip this check.");
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
}
