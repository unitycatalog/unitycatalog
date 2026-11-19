package io.unitycatalog.server.base.externallocation;

import static io.unitycatalog.server.base.credential.BaseCredentialCRUDTest.NOT_AN_HTTP_URL;
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
import io.unitycatalog.client.model.S3AddressingStyle;
import io.unitycatalog.client.model.S3CompatibleAccessKeyRequest;
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
    createCompatCredential(COMPAT_CREDENTIAL_NAME);
    createCompatCredential(ANOTHER_COMPAT_CREDENTIAL_NAME);

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
          () -> createLocation("el_other", url, COMPAT_CREDENTIAL_NAME, endpointA),
          ErrorCode.INVALID_ARGUMENT,
          "its external locations must be s3:// paths");
    }
    // A location has an endpoint exactly when its credential is s3_compatible_access_key.
    assertApiException(
        () -> createLocation("el_other", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME, null),
        ErrorCode.INVALID_ARGUMENT,
        "its external locations need the endpoint of their storage");
    assertApiException(
        () -> createLocation("el_other", "s3://shared-bucket/a", CREDENTIAL_NAME, endpointA),
        ErrorCode.INVALID_ARGUMENT,
        "is not an s3_compatible_access_key credential, so its external locations have no"
            + " endpoint");
    for (InvalidEndpoint invalid : INVALID_ENDPOINTS) {
      assertApiException(
          () ->
              createLocation(
                  "el_other", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME, invalid.endpoint()),
          ErrorCode.INVALID_ARGUMENT,
          "Invalid endpoint:" + invalid.error());
    }
    // A rejected value is left out of the message, since it may carry something secret.
    for (String endpoint :
        List.of("https://user:value@storage.example.com/%ZZ", "user:value@storage.example.com")) {
      ApiException e =
          assertThrows(
              ApiException.class,
              () ->
                  createLocation(
                      "el_other", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME, endpoint));
      assertThat(e.getResponseBody()).doesNotContain("user:value");
    }
    assertThat(externalLocationOperations.listExternalLocations(Optional.empty()))
        .noneMatch(location -> location.getName().equals("el_other"));

    // A bucket name S3 allows that a URI host does not, such as one with an underscore.
    createLocation("el_underscore", "s3://my_bucket/data", COMPAT_CREDENTIAL_NAME, endpointA);
    delete("el_underscore", Optional.empty());

    // One credential, and one bucket name, on two storage systems, under locations that do not
    // overlap.
    ExternalLocationInfo locationA =
        createLocation("el_a", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME, endpointA);
    assertThat(locationA.getEndpoint()).isEqualTo(endpointA);
    createLocation("el_b", "s3://shared-bucket/b", COMPAT_CREDENTIAL_NAME, endpointB);
    assertThat(externalLocationOperations.getExternalLocation("el_b").getEndpoint())
        .isEqualTo(endpointB);
    assertApiException(
        () -> createLocation("el_bucket", "s3://shared-bucket", COMPAT_CREDENTIAL_NAME, endpointA),
        ErrorCode.INVALID_ARGUMENT,
        "overlaps with existing external location");

    // Updates keep the same rules: a new URL, a new endpoint, or a new credential. An update
    // without an endpoint keeps the stored one.
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                "el_a", new UpdateExternalLocation().url("gs://shared-bucket/a")),
        ErrorCode.INVALID_ARGUMENT,
        "its external locations must be s3:// paths");
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                "el_a", new UpdateExternalLocation().endpoint("http://storage-a.invalid:9000/s3")),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid endpoint: must be http[s]://host[:port], without a path.");
    assertThat(
            externalLocationOperations
                .updateExternalLocation("el_a", new UpdateExternalLocation().comment("moved"))
                .getEndpoint())
        .isEqualTo(endpointA);
    String movedEndpoint = "http://storage-a-moved.invalid:9000";
    assertThat(
            externalLocationOperations
                .updateExternalLocation(
                    "el_a", new UpdateExternalLocation().endpoint(movedEndpoint))
                .getEndpoint())
        .isEqualTo(movedEndpoint);
    // To another s3_compatible_access_key credential, keeping the endpoint, but not to a
    // credential of another type, nor from one.
    ExternalLocationInfo moved =
        externalLocationOperations.updateExternalLocation(
            "el_a", new UpdateExternalLocation().credentialName(ANOTHER_COMPAT_CREDENTIAL_NAME));
    assertThat(moved.getCredentialName()).isEqualTo(ANOTHER_COMPAT_CREDENTIAL_NAME);
    assertThat(moved.getEndpoint()).isEqualTo(movedEndpoint);
    create(EXTERNAL_LOCATION_NAME, testUrl());
    for (UpdateExternalLocationCase c :
        List.of(
            new UpdateExternalLocationCase("el_a", CREDENTIAL_NAME),
            new UpdateExternalLocationCase(EXTERNAL_LOCATION_NAME, COMPAT_CREDENTIAL_NAME))) {
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  c.location(), new UpdateExternalLocation().credentialName(c.credential())),
          ErrorCode.INVALID_ARGUMENT,
          "An external location cannot move between an s3_compatible_access_key credential and a"
              + " credential of another type: "
              + c.credential());
    }
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME, new UpdateExternalLocation().endpoint(endpointA)),
        ErrorCode.INVALID_ARGUMENT,
        "so its external locations have no endpoint");

    // Nor does a credential change type while a location uses it; unused, it may.
    for (UpdateCredentialCase c :
        List.of(
            new UpdateCredentialCase(
                ANOTHER_COMPAT_CREDENTIAL_NAME,
                new UpdateCredentialRequest()
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN))),
            new UpdateCredentialCase(
                CREDENTIAL_NAME, new UpdateCredentialRequest().s3CompatibleAccessKey(keyPair())))) {
      assertApiException(
          () -> credentialOperations.updateCredential(c.credential(), c.update()),
          ErrorCode.INVALID_ARGUMENT,
          "Cannot change the type of credential '"
              + c.credential()
              + "' while an external location uses it.");
    }
    for (String name : List.of("el_a", "el_b")) {
      delete(name, Optional.empty());
    }
    credentialOperations.updateCredential(
        COMPAT_CREDENTIAL_NAME,
        new UpdateCredentialRequest().awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN)));
    for (String name : List.of(COMPAT_CREDENTIAL_NAME, ANOTHER_COMPAT_CREDENTIAL_NAME)) {
      credentialOperations.deleteCredential(name, Optional.empty());
    }
  }

  @Test
  public void testS3CompatibleAddressingStyle() throws ApiException {
    String endpoint = "http://storage.invalid:9000";
    createCompatCredential(COMPAT_CREDENTIAL_NAME);

    // Path-style unless the location says otherwise, and only with an endpoint.
    assertThat(
            createLocation("el_path", "s3://path-bucket/a", COMPAT_CREDENTIAL_NAME, endpoint)
                .getAddressingStyle())
        .isEqualTo(S3AddressingStyle.PATH);
    assertThat(externalLocationOperations.getExternalLocation("el_path").getAddressingStyle())
        .isEqualTo(S3AddressingStyle.PATH);
    assertThat(create(EXTERNAL_LOCATION_NAME, testUrl()).getAddressingStyle()).isNull();
    String noStyle = "so its external locations have no endpoint or addressing style";
    assertApiException(
        () ->
            createLocation(
                "el_other", anotherTestUrl(), CREDENTIAL_NAME, null, S3AddressingStyle.PATH),
        ErrorCode.INVALID_ARGUMENT,
        noStyle);
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                EXTERNAL_LOCATION_NAME,
                new UpdateExternalLocation().addressingStyle(S3AddressingStyle.PATH)),
        ErrorCode.INVALID_ARGUMENT,
        noStyle);

    // Virtual-hosted puts the bucket in the host name, so its name must be a host name label.
    for (String bucket :
        List.of("my_bucket", "data.2024", "ab", "-bucket", "bucket-", "b".repeat(64))) {
      assertApiException(
          () ->
              createLocation(
                  "el_other",
                  "s3://" + bucket + "/a",
                  COMPAT_CREDENTIAL_NAME,
                  endpoint,
                  S3AddressingStyle.VIRTUAL_HOSTED),
          ErrorCode.INVALID_ARGUMENT,
          "Bucket '" + bucket + "' cannot be addressed VIRTUAL_HOSTED");
    }
    ExternalLocationInfo hosted =
        createLocation(
            "el_hosted",
            "s3://hosted-bucket-1/a",
            COMPAT_CREDENTIAL_NAME,
            endpoint,
            S3AddressingStyle.VIRTUAL_HOSTED);
    assertThat(hosted.getAddressingStyle()).isEqualTo(S3AddressingStyle.VIRTUAL_HOSTED);
    // An update without a style keeps it, so a new URL must qualify too.
    assertThat(
            externalLocationOperations
                .updateExternalLocation("el_hosted", new UpdateExternalLocation().comment("c"))
                .getAddressingStyle())
        .isEqualTo(S3AddressingStyle.VIRTUAL_HOSTED);
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                "el_hosted", new UpdateExternalLocation().url("s3://hosted.bucket/a")),
        ErrorCode.INVALID_ARGUMENT,
        "Bucket 'hosted.bucket' cannot be addressed VIRTUAL_HOSTED");
    // Path-style takes any bucket name S3 allows; switching it needs one that qualifies.
    createLocation("el_dotted", "s3://data.2024/a", COMPAT_CREDENTIAL_NAME, endpoint);
    assertApiException(
        () ->
            externalLocationOperations.updateExternalLocation(
                "el_dotted",
                new UpdateExternalLocation().addressingStyle(S3AddressingStyle.VIRTUAL_HOSTED)),
        ErrorCode.INVALID_ARGUMENT,
        "Bucket 'data.2024' cannot be addressed VIRTUAL_HOSTED");
    for (S3AddressingStyle style :
        List.of(S3AddressingStyle.VIRTUAL_HOSTED, S3AddressingStyle.PATH)) {
      assertThat(
              externalLocationOperations
                  .updateExternalLocation(
                      "el_path", new UpdateExternalLocation().addressingStyle(style))
                  .getAddressingStyle())
          .isEqualTo(style);
    }
    for (String name : List.of("el_path", "el_hosted", "el_dotted")) {
      delete(name, Optional.empty());
    }
    credentialOperations.deleteCredential(COMPAT_CREDENTIAL_NAME, Optional.empty());
  }

  private record InvalidEndpoint(String endpoint, String error) {}

  private record UpdateExternalLocationCase(String location, String credential) {}

  private record UpdateCredentialCase(String credential, UpdateCredentialRequest update) {}

  private static final List<InvalidEndpoint> INVALID_ENDPOINTS =
      List.of(
          new InvalidEndpoint("storage.example.com:9000", NOT_AN_HTTP_URL),
          // java.net.URI finds no host in a name with an underscore, and neither would a client.
          new InvalidEndpoint("http://minio_1:9000", NOT_AN_HTTP_URL),
          new InvalidEndpoint("http://storage.example.com:0", NOT_AN_HTTP_URL),
          new InvalidEndpoint("http://storage.example.com:65536", NOT_AN_HTTP_URL),
          // The S3 API is at the root of the host, named exactly.
          new InvalidEndpoint(
              "https://storage.example.com:9000/s3",
              " must be http[s]://host[:port], without a path."),
          new InvalidEndpoint(
              "https://storage.example.com:9000/",
              " must be http[s]://host[:port], without a path."),
          new InvalidEndpoint(
              "https://storage.example.com:", " must be http[s]://host[:port], without a path."),
          new InvalidEndpoint("https://user:value@storage.example.com/%ZZ", " not a URL."),
          new InvalidEndpoint("user:value@storage.example.com", NOT_AN_HTTP_URL),
          // Endpoints are readable by everyone who can read the location.
          new InvalidEndpoint(
              "https://user:value@storage.example.com",
              " must not contain user info, a query or a fragment."),
          new InvalidEndpoint(
              "https://storage.example.com/#value",
              " must not contain user info, a query or a fragment."));

  @Test
  public void testLocationWithAUrlFromAnEarlierReleaseCanStillChange() throws ApiException {
    // An earlier release stored URLs that no longer parse, such as one with a query. Changing
    // anything but the URL still works, as does moving it to another credential that is not
    // s3_compatible_access_key.
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

    // Nor is a stored s3:// URL that no longer parses. The credential's error leaves it out, as
    // managing a credential does not grant reading its locations, and moving the location to an
    // s3_compatible_access_key credential is refused rather than failing in the parser.
    createCompatCredential(COMPAT_CREDENTIAL_NAME);
    for (String storedUrl : List.of("s3://secret-bucket/a b", "S3://secret-bucket/x", "s3:///")) {
      setStoredUrl(NEW_EXTERNAL_LOCATION_NAME, storedUrl);
      ApiException credentialChange =
          assertThrows(
              ApiException.class,
              () ->
                  credentialOperations.updateCredential(
                      ANOTHER_CREDENTIAL_NAME,
                      new UpdateCredentialRequest().s3CompatibleAccessKey(keyPair())));
      assertThat(credentialChange.getCode()).as(storedUrl).isEqualTo(400);
      assertThat(credentialChange.getResponseBody())
          .as(storedUrl)
          .contains("Cannot change the type of credential '" + ANOTHER_CREDENTIAL_NAME + "'")
          .doesNotContain("secret-bucket");
      assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  NEW_EXTERNAL_LOCATION_NAME,
                  new UpdateExternalLocation()
                      .credentialName(COMPAT_CREDENTIAL_NAME)
                      .endpoint("http://s3:9000")),
          ErrorCode.INVALID_ARGUMENT,
          "An external location cannot move between an s3_compatible_access_key credential");
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

  protected void createCompatCredential(String name) throws ApiException {
    credentialOperations.createCredential(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .s3CompatibleAccessKey(keyPair()));
  }

  /**
   * @param endpoint the location's S3 endpoint, or null for none
   */
  protected ExternalLocationInfo createLocation(
      String name, String url, String credentialName, String endpoint) throws ApiException {
    return createLocation(name, url, credentialName, endpoint, /* addressingStyle= */ null);
  }

  /**
   * @param addressingStyle the location's addressing style, or null to leave it out
   */
  protected ExternalLocationInfo createLocation(
      String name,
      String url,
      String credentialName,
      String endpoint,
      S3AddressingStyle addressingStyle)
      throws ApiException {
    ExternalLocationInfo created =
        externalLocationOperations.createExternalLocation(
            new CreateExternalLocation()
                .name(name)
                .url(url)
                .credentialName(credentialName)
                .endpoint(endpoint)
                .addressingStyle(addressingStyle));
    externalLocationsToDelete.add(name);
    return created;
  }

  protected static S3CompatibleAccessKeyRequest keyPair() {
    return new S3CompatibleAccessKeyRequest()
        .accessKeyId("key-id-1")
        .secretAccessKey("key-value-1");
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
