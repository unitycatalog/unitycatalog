package io.unitycatalog.server.persist.utils;

import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_CATALOG_PREFIX;
import static io.unitycatalog.server.utils.Constants.MANAGED_STORAGE_SCHEMA_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageLimits;
import io.unitycatalog.server.service.credential.StorageLimits.Extent;
import io.unitycatalog.server.service.credential.StorageLimits.Unit;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class StorageLocationValidatorTest {

  private static final String STS_BUCKET = "s3://sts-bucket";
  private static final String STATIC_BUCKET = "s3://static-bucket";
  private static final String CUSTOM_S3_BUCKET = "s3://custom-s3-bucket";
  private static final String GCS_BUCKET = "gs://gcs-bucket";
  private static final String CUSTOM_GCS_BUCKET = "gs://custom-gcs-bucket";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/bucket-role";
  private static final Set<CredentialContext.Privilege> READ =
      Set.of(CredentialContext.Privilege.SELECT);
  private static final Set<CredentialContext.Privilege> READ_WRITE =
      Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);
  private static final EncryptedCredentialSecretStore SECRET_STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  private final List<AssumeRoleRequest> stsRequests = new ArrayList<>();
  private final CloudCredentialVendor cloudCredentialVendor = cloudCredentialVendor();

  private CloudCredentialVendor cloudCredentialVendor() {
    Properties properties = new Properties();
    // The master role, for aws_iam_role credentials, with no lookup of the environment's region.
    properties.put("aws.region", "us-west-2");
    properties.put("aws.accessKey", "masterKey");
    properties.put("aws.secretKey", "masterSecret");
    properties.put("gcs.bucketPath.0", GCS_BUCKET);
    properties.put("gcs.bucketPath.1", CUSTOM_GCS_BUCKET);
    properties.put("gcs.credentialGenerator.1", "com.example.CustomGenerator");
    properties.put("s3.bucketPath.0", STS_BUCKET);
    properties.put("s3.region.0", "us-west-2");
    properties.put("s3.awsRoleArn.0", ROLE_ARN);
    properties.put("s3.accessKey.0", "accessKey0");
    properties.put("s3.secretKey.0", "secretKey0");
    properties.put("s3.bucketPath.1", STATIC_BUCKET);
    properties.put("s3.region.1", "us-west-2");
    properties.put("s3.accessKey.1", "accessKey1");
    properties.put("s3.secretKey.1", "secretKey1");
    properties.put("s3.sessionToken.1", "sessionToken1");
    properties.put("s3.bucketPath.2", CUSTOM_S3_BUCKET);
    properties.put("s3.region.2", "us-west-2");
    properties.put("s3.awsRoleArn.2", ROLE_ARN);
    properties.put("s3.credentialGenerator.2", "com.example.CustomGenerator");
    ServerProperties serverProperties = new ServerProperties(properties);

    StsClient stsClient = mock(StsClient.class);
    when(stsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              stsRequests.add(invocation.getArgument(0));
              return AssumeRoleResponse.builder()
                  .credentials(
                      Credentials.builder()
                          .accessKeyId("vendedKey")
                          .secretAccessKey("vendedSecret")
                          .sessionToken("vendedToken")
                          .expiration(Instant.now().plusSeconds(3600))
                          .build())
                  .build();
            });
    StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
    when(stsClientBuilder.region(any())).thenReturn(stsClientBuilder);
    when(stsClientBuilder.credentialsProvider(any())).thenReturn(stsClientBuilder);
    when(stsClientBuilder.build()).thenReturn(stsClient);
    return new CloudCredentialVendor(
        new AwsCredentialVendor(serverProperties, () -> stsClientBuilder),
        new AzureCredentialVendor(serverProperties),
        new GcpCredentialVendor(serverProperties));
  }

  private StorageLocationValidator validatorFor(Optional<CredentialDAO> credential) {
    return new StorageLocationValidator(
        cloudCredentialVendor, (session, location) -> credential, () -> Unit.UTF16_CODE_UNITS);
  }

  private static Optional<BaseException> rootRejection(
      StorageLocationValidator validator, NormalizedURL root, String prefix) {
    try {
      if (prefix.equals(MANAGED_STORAGE_CATALOG_PREFIX)) {
        validator.validateCatalogStorageRoot(null, root);
      } else {
        validator.validateSchemaStorageRoot(null, root);
      }
      return Optional.empty();
    } catch (BaseException e) {
      assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
      return Optional.of(e);
    }
  }

  /** Every kind of managed location the server creates under a catalog or schema root. */
  private static Stream<NormalizedURL> managedLocationsUnder(NormalizedURL root, String prefix) {
    UUID id = UUID.randomUUID();
    NormalizedURL container =
        prefix.equals(MANAGED_STORAGE_CATALOG_PREFIX)
            ? ExternalLocationUtils.getManagedLocationForCatalog(root, id)
            : ExternalLocationUtils.getManagedLocationForSchema(root, id);
    NormalizedURL model = ExternalLocationUtils.getManagedLocationForModel(container, id);
    return Stream.of(
        ExternalLocationUtils.getManagedLocationForTable(container, id),
        ExternalLocationUtils.getManagedLocationForVolume(container, id),
        model,
        ExternalLocationUtils.getManagedLocationForModelVersion(model, id));
  }

  private boolean canVend(NormalizedURL location, Optional<CredentialDAO> credential) {
    return canVend(location, READ_WRITE, credential);
  }

  private boolean canVend(
      NormalizedURL location,
      Set<CredentialContext.Privilege> privileges,
      Optional<CredentialDAO> credential) {
    try {
      cloudCredentialVendor.vendCredential(
          CredentialContext.create(location, privileges, credential));
      return true;
    } catch (BaseException e) {
      assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
      return false;
    }
  }

  private void assertAcceptedExactlyWhenVendable(
      String bucket, String prefix, Optional<CredentialDAO> credential) {
    StorageLocationValidator validator = validatorFor(credential);
    List<Integer> accepted =
        IntStream.rangeClosed(1, 600)
            .filter(
                length -> {
                  NormalizedURL root = NormalizedURL.from(bucket + "/" + "r".repeat(length));
                  boolean vendable =
                      managedLocationsUnder(root, prefix).allMatch(l -> canVend(l, credential));
                  boolean isAccepted = rootRejection(validator, root, prefix).isEmpty();
                  assertThat(isAccepted)
                      .as("root path of %d characters", length)
                      .isEqualTo(vendable);
                  return isAccepted;
                })
            .boxed()
            .toList();
    assertThat(accepted).isNotEmpty();
    int longestAccepted = accepted.get(accepted.size() - 1);
    assertThat(longestAccepted).isLessThan(600);
    assertThat(accepted).hasSize(longestAccepted);
  }

  @ParameterizedTest
  @ValueSource(strings = {MANAGED_STORAGE_CATALOG_PREFIX, MANAGED_STORAGE_SCHEMA_PREFIX})
  public void testAwsRootIsAcceptedExactlyWhenEveryManagedLocationCanBeVended(String prefix) {
    assertAcceptedExactlyWhenVendable(STS_BUCKET, prefix, Optional.empty());

    // AWS STS: "The plaintext that you use for both inline and managed session policies can't
    // exceed 2,048 characters." (API_AssumeRole, checked 2026-11-21). Each root character adds
    // five to the policy, so the longest policy sent is within five of the limit.
    assertThat(stsRequests).isNotEmpty();
    assertThat(stsRequests.stream().mapToInt(r -> r.policy().length()).max().getAsInt())
        .isBetween(2044, 2048);
  }

  @Test
  public void testAwsIamRoleCredentialRootIsAcceptedExactlyWhenEveryManagedLocationCanBeVended() {
    CredentialDAO awsIamRole =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("role-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN)),
            "test-user",
            SECRET_STORE);
    assertAcceptedExactlyWhenVendable(
        "s3://external-bucket", MANAGED_STORAGE_CATALOG_PREFIX, Optional.of(awsIamRole));
  }

  @Test
  public void testRequestLocationNeedsOnlyItsReadPolicyToFit() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    List<Integer> readOnlyLengths =
        IntStream.rangeClosed(1, 600)
            .filter(
                length -> {
                  NormalizedURL location =
                      NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(length));
                  boolean accepted =
                      accepts(
                          () ->
                              validator.validateLocation(
                                  null, location, 2048, Extent.OBJECT_OR_PREFIX));
                  boolean acceptedAsManaged =
                      accepts(() -> validator.validateManagedLocation(null, location, 2048));
                  assertThat(accepted)
                      .as("location path of %d characters", length)
                      .isEqualTo(canVend(location, READ, Optional.empty()));
                  assertThat(acceptedAsManaged)
                      .as("managed location path of %d characters", length)
                      .isEqualTo(canVend(location, READ_WRITE, Optional.empty()));
                  return accepted && !acceptedAsManaged;
                })
            .boxed()
            .toList();

    // Between the two policy sizes, a location can be registered and read, and a write vend fails
    // with INVALID_ARGUMENT.
    assertThat(readOnlyLengths).isNotEmpty();
    assertThat(
            accepts(
                () ->
                    validator.validateLocation(
                        null,
                        NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(600)),
                        2048,
                        Extent.OBJECT_OR_PREFIX)))
        .isFalse();
  }

  private static boolean accepts(Runnable validation) {
    try {
      validation.run();
      return true;
    } catch (BaseException e) {
      assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
      return false;
    }
  }

  @Test
  public void testTooLongAwsSessionPolicyIsRejectedWithoutCallingSts() {
    NormalizedURL location = NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(600));

    assertThatThrownBy(
            () ->
                cloudCredentialVendor.vendCredential(
                    CredentialContext.create(location, READ_WRITE, Optional.empty())))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("its AWS STS session policy is")
        .hasMessageContaining("and the limit is 2048")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    assertThat(stsRequests).isEmpty();
  }

  @Test
  public void testS3CompatibleStorageIsLimitedOnlyByTheColumn() {
    CredentialDAO s3Compatible =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("compat-credential")
                .purpose(CredentialPurpose.STORAGE)
                .s3Compatible(
                    new S3CompatibleRequest()
                        .s3Endpoint("http://storage.example.com:9000")
                        .accessKeyId("key-id")
                        .secretAccessKey("key-value")
                        .assumeRole(new S3AssumeRole().roleArn(ROLE_ARN))),
            "test-user",
            SECRET_STORE);
    NormalizedURL root = NormalizedURL.from("s3://compat-bucket/" + "r".repeat(1500));

    assertThat(
            rootRejection(
                validatorFor(Optional.of(s3Compatible)), root, MANAGED_STORAGE_CATALOG_PREFIX))
        .isEmpty();
  }

  @Test
  public void testStaticS3BucketIsLimitedByTheObjectKeyLength() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    int longestAccepted =
        IntStream.rangeClosed(1, 1100)
            .filter(
                length ->
                    rootRejection(
                            validator,
                            NormalizedURL.from(STATIC_BUCKET + "/" + "r".repeat(length)),
                            MANAGED_STORAGE_CATALOG_PREFIX)
                        .isEmpty())
            .max()
            .getAsInt();
    NormalizedURL longestRoot =
        NormalizedURL.from(STATIC_BUCKET + "/" + "r".repeat(longestAccepted));

    // S3: object keys are "a maximum length of 1,024 bytes" (Amazon S3 object keys, checked
    // 2026-11-21). An object directly under the deepest managed location fits exactly.
    int deepestKeyBytes =
        managedLocationsUnder(longestRoot, MANAGED_STORAGE_CATALOG_PREFIX)
            .mapToInt(l -> StorageLimits.objectName(l).length())
            .max()
            .getAsInt();
    assertThat(deepestKeyBytes + "/x".length()).isEqualTo(1024);
    assertThat(
            rootRejection(
                validator,
                NormalizedURL.from(STATIC_BUCKET + "/" + "r".repeat(longestAccepted + 1)),
                MANAGED_STORAGE_CATALOG_PREFIX))
        .get()
        .extracting(Throwable::getMessage)
        .isEqualTo(
            "Storage root is too long. For the location of a managed model version under it, S3"
                + " object key names under it have at least 1025 bytes, and the limit is 1024.");
  }

  @Test
  public void testAzureBlobNamesAreCountedInCharacters() {
    String container = "abfss://container@account.dfs.core.windows.net/";
    StorageLocationValidator validator = validatorFor(Optional.empty());
    int longestAccepted =
        IntStream.rangeClosed(1, 1100)
            .filter(
                length ->
                    rootRejection(
                            validator,
                            NormalizedURL.from(container + "é".repeat(length)),
                            MANAGED_STORAGE_SCHEMA_PREFIX)
                        .isEmpty())
            .max()
            .getAsInt();
    NormalizedURL longestRoot = NormalizedURL.from(container + "é".repeat(longestAccepted));

    // Azure: "A blob name ... cannot be more than 1,024 characters long" (Naming and Referencing
    // Containers, Blobs, and Metadata, checked 2026-11-21).
    int deepestNameCharacters =
        managedLocationsUnder(longestRoot, MANAGED_STORAGE_SCHEMA_PREFIX)
            .mapToInt(l -> StorageLimits.objectName(l).length())
            .max()
            .getAsInt();
    assertThat(deepestNameCharacters + "/x".length()).isEqualTo(1024);
  }

  @Test
  public void testAzurePathSegmentsAreLimited() {
    String container = "abfss://container@account.dfs.core.windows.net/";
    StorageLocationValidator validator = validatorFor(Optional.empty());
    int mostSegmentsAccepted =
        IntStream.rangeClosed(1, 300)
            .filter(
                segments ->
                    rootRejection(
                            validator,
                            NormalizedURL.from(
                                container + String.join("/", Collections.nCopies(segments, "s"))),
                            MANAGED_STORAGE_CATALOG_PREFIX)
                        .isEmpty())
            .max()
            .getAsInt();
    // Azure: "the number of path segments comprising the blob name cannot exceed 254" (Naming and
    // Referencing Containers, Blobs, and Metadata, checked 2026-11-21). The deepest managed
    // location under a catalog root, __unitystorage/catalogs/<id>/models/<id>/versions/<id>, adds
    // seven segments, and a blob under it one more.
    assertThat(mostSegmentsAccepted + 7 + 1).isEqualTo(254);
  }

  private void assertStorageRootTooLong(
      String storageRoot, String prefix, int rootBytes, int maxRootBytes) {
    assertThat(
            rootRejection(validatorFor(Optional.empty()), NormalizedURL.from(storageRoot), prefix))
        .get()
        .extracting(Throwable::getMessage)
        .asString()
        .startsWith(
            String.format(
                "Storage root is %d bytes long in UTF-8, but it can be at most %d bytes",
                rootBytes, maxRootBytes));
  }

  private void assertStorageRootAccepted(String storageRoot, String prefix) {
    assertThat(
            rootRejection(validatorFor(Optional.empty()), NormalizedURL.from(storageRoot), prefix))
        .isEmpty();
  }

  @Test
  public void testManagedLocationsUnderTheLongestStorageRootFitTheirColumns() {
    UUID id = UUID.randomUUID();
    String bucket = "s3://bucket/";

    String catalogRoot = bucket + "c".repeat(1942 - bucket.length());
    assertStorageRootAccepted(catalogRoot, MANAGED_STORAGE_CATALOG_PREFIX);
    NormalizedURL catalogLocation =
        ExternalLocationUtils.getManagedLocationForCatalog(NormalizedURL.from(catalogRoot), id);
    assertThat(ExternalLocationUtils.getManagedLocationForVolume(catalogLocation, id).toString())
        .hasSize(2048);
    assertStorageRootTooLong(catalogRoot + "c", MANAGED_STORAGE_CATALOG_PREFIX, 1943, 1942);

    String schemaRoot = bucket + "s".repeat(1943 - bucket.length());
    assertStorageRootAccepted(schemaRoot, MANAGED_STORAGE_SCHEMA_PREFIX);
    NormalizedURL schemaLocation =
        ExternalLocationUtils.getManagedLocationForSchema(NormalizedURL.from(schemaRoot), id);
    assertThat(ExternalLocationUtils.getManagedLocationForVolume(schemaLocation, id).toString())
        .hasSize(2048);
    assertStorageRootTooLong(schemaRoot + "s", MANAGED_STORAGE_SCHEMA_PREFIX, 1944, 1943);

    String twoByteCharacters = "é".repeat((1942 - bucket.length()) / 2);
    assertStorageRootAccepted(bucket + twoByteCharacters, MANAGED_STORAGE_CATALOG_PREFIX);
    assertStorageRootTooLong(
        bucket + twoByteCharacters + "é", MANAGED_STORAGE_CATALOG_PREFIX, 1944, 1942);
  }

  @Test
  public void testExplicitLocationIsCheckedInCharacters() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String bucket = "file:///tmp/";
    NormalizedURL longest = NormalizedURL.from(bucket + "é".repeat(2048 - bucket.length()));

    // The columns count characters, so a location that fits in characters but not in bytes is
    // still accepted, as on main.
    assertThatCode(() -> validator.validateLocation(null, longest, 2048, Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(longest + "é"), 2048, Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage("Storage location is too long: it is 2049 characters, and the limit is 2048.");
  }

  @Test
  public void testExplicitLocationIsCountedTheWayTheDatabaseCountsIt() {
    // An emoji is one code point and two UTF-16 code units.
    String emoji = new String(Character.toChars(0x1F600));
    NormalizedURL url = NormalizedURL.from("s3://bucket/" + emoji.repeat(122));
    StorageLocationValidator codePoints =
        new StorageLocationValidator(
            cloudCredentialVendor, (session, location) -> Optional.empty(), () -> Unit.CODE_POINTS);

    assertThatCode(() -> codePoints.validateExternalLocationUrl(url, 255, Optional.empty()))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                codePoints.validateExternalLocationUrl(
                    NormalizedURL.from(url + "a".repeat(122)), 255, Optional.empty()))
        .isInstanceOf(BaseException.class)
        .hasMessage("Storage location is too long: it is 256 characters, and the limit is 255.");
    assertThatThrownBy(
            () ->
                validatorFor(Optional.empty())
                    .validateExternalLocationUrl(url, 255, Optional.empty()))
        .isInstanceOf(BaseException.class)
        .hasMessage("Storage location is too long: it is 256 characters, and the limit is 255.");
  }

  @Test
  public void testManagedLocationIsCheckedInBytes() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String bucket = "file:///tmp/";
    NormalizedURL longest = NormalizedURL.from(bucket + "é".repeat((2048 - bucket.length()) / 2));

    assertThatCode(() -> validator.validateManagedLocation(null, longest, 2048))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () -> validator.validateManagedLocation(null, NormalizedURL.from(longest + "é"), 2048))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: it is 2050 bytes in UTF-8, and the limit is 2048.");
  }

  @Test
  public void testRegisteredModelIsRejectedWhenItsVersionsCannotFit() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String container = "abfss://container@account.dfs.core.windows.net/";
    UUID id = UUID.randomUUID();
    int longestAccepted =
        IntStream.rangeClosed(1, 1100)
            .filter(
                length -> {
                  try {
                    validator.validateModelLocation(
                        null, NormalizedURL.from(container + "m".repeat(length)));
                    return true;
                  } catch (BaseException e) {
                    return false;
                  }
                })
            .max()
            .getAsInt();
    NormalizedURL version =
        ExternalLocationUtils.getManagedLocationForModelVersion(
            NormalizedURL.from(container + "m".repeat(longestAccepted)), id);
    assertThat(StorageLimits.objectName(version).length() + "/x".length()).isEqualTo(1024);
    assertThatThrownBy(
            () ->
                validator.validateModelLocation(
                    null, NormalizedURL.from(container + "m".repeat(longestAccepted + 1))))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Registered model location is too long. For the location of a model version under"
                + " it, Azure blob names under it have at least 1025 characters, and the limit is"
                + " 1024.");
  }

  @Test
  public void testOnlyASingleFileRequestLocationMayNameAnS3Object() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    // An external table can be a single file, so a location given in a request only has to be a
    // valid key itself. S3: object keys are "a maximum length of 1,024 bytes" (Amazon S3 object
    // keys, checked 2026-11-21).
    NormalizedURL file = NormalizedURL.from(STATIC_BUCKET + "/" + "a".repeat(1020) + ".csv");
    assertThat(StorageLimits.objectName(file)).hasSize(1024);

    assertThatCode(() -> validator.validateLocation(null, file, 2048, Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null,
                    NormalizedURL.from(STATIC_BUCKET + "/" + "a".repeat(1021) + ".csv"),
                    2048,
                    Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: its S3 object key name has 1025 bytes, and the limit"
                + " is 1024.");
    // A file has to fit under a managed location or a request location that is a directory.
    String noRoomForAFile =
        "Storage location is too long: S3 object key names under it have at least 1026"
            + " bytes, and the limit is 1024.";
    assertThatThrownBy(() -> validator.validateManagedLocation(null, file, 2048))
        .isInstanceOf(BaseException.class)
        .hasMessage(noRoomForAFile);
    assertThatThrownBy(() -> validator.validateLocation(null, file, 2048, Extent.PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(noRoomForAFile);
    NormalizedURL directory = NormalizedURL.from(STATIC_BUCKET + "/" + "a".repeat(1022));
    assertThat(StorageLimits.objectName(directory)).hasSize(1022);
    assertThatCode(() -> validator.validateLocation(null, directory, 2048, Extent.PREFIX))
        .doesNotThrowAnyException();
  }

  @Test
  public void testOnlyASingleFileRequestLocationMayNameAnAzureBlob() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String container = "abfss://container@account.dfs.core.windows.net/";
    NormalizedURL deepest =
        NormalizedURL.from(container + String.join("/", Collections.nCopies(254, "s")));
    NormalizedURL longest = NormalizedURL.from(container + "b".repeat(1024));

    assertThatCode(() -> validator.validateLocation(null, deepest, 2048, Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(deepest + "/s"), 2048, Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: its Azure blob name has 255 path segments, and the"
                + " limit is 254.");
    assertThatCode(() -> validator.validateLocation(null, longest, 2048, Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(longest + "b"), 2048, Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: its Azure blob name has 1025 characters, and the limit"
                + " is 1024.");

    // A directory needs room for one more segment and "/x".
    assertThatThrownBy(() -> validator.validateLocation(null, deepest, 2048, Extent.PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: Azure blob names under it have at least 255 path"
                + " segments, and the limit is 254.");
    NormalizedURL deepestDirectory =
        NormalizedURL.from(container + String.join("/", Collections.nCopies(253, "s")));
    assertThatCode(() -> validator.validateLocation(null, deepestDirectory, 2048, Extent.PREFIX))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(container + "b".repeat(1023)), 2048, Extent.PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: Azure blob names under it have at least 1025"
                + " characters, and the limit is 1024.");
    assertThatCode(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(container + "b".repeat(1022)), 2048, Extent.PREFIX))
        .doesNotThrowAnyException();
  }

  @Test
  public void testExternalLocationNeedsRoomForAFileUnderIt() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String container = "abfss://container@account.dfs.core.windows.net/";

    assertThatCode(
            () ->
                validator.validateExternalLocationUrl(
                    NormalizedURL.from(container + "b".repeat(1022)), 2048, Optional.empty()))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                validator.validateExternalLocationUrl(
                    NormalizedURL.from(container + "b".repeat(1023)), 2048, Optional.empty()))
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "Storage location is too long: Azure blob names under it have at least 1025"
                + " characters, and the limit is 1024.");
  }

  @ParameterizedTest
  @EnumSource(DataSourceFormat.class)
  public void testOnlyDeltaAndIcebergTablesAreAlwaysDirectories(DataSourceFormat format) {
    boolean directory = format == DataSourceFormat.DELTA || format == DataSourceFormat.ICEBERG;
    assertThat(Extent.ofTable(format))
        .isEqualTo(directory ? Extent.PREFIX : Extent.OBJECT_OR_PREFIX);
    assertThat(Extent.ofTable(null)).isEqualTo(Extent.OBJECT_OR_PREFIX);
  }

  @Test
  public void testS3BucketsTheServerDoesNotVendForAreOnlyLimitedByTheColumn() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String path = "/" + "r".repeat(1100);

    // A custom generator vends credentials its own way, and the server vends none for a bucket it
    // has no configuration for.
    for (String bucket : List.of(CUSTOM_S3_BUCKET, "s3://unconfigured-bucket")) {
      assertThat(
              rootRejection(
                  validator, NormalizedURL.from(bucket + path), MANAGED_STORAGE_CATALOG_PREFIX))
          .as(bucket)
          .isEmpty();
    }
    assertThat(
            rootRejection(
                validator, NormalizedURL.from(STS_BUCKET + path), MANAGED_STORAGE_CATALOG_PREFIX))
        .isPresent();
  }

  @Test
  public void testFailingCredentialLookupLeavesAzureLimitsChecked() {
    StorageLocationValidator validator =
        new StorageLocationValidator(
            cloudCredentialVendor,
            (session, location) -> {
              throw new BaseException(ErrorCode.INTERNAL, "The lookup failed");
            },
            () -> Unit.UTF16_CODE_UNITS);

    // Azure vends without a storage credential, so its limits don't depend on the lookup.
    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null,
                    NormalizedURL.from(
                        "abfss://container@account.dfs.core.windows.net/" + "a".repeat(1100)),
                    2048,
                    Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("its Azure blob name has 1100 characters");
  }

  @Test
  public void testUnresolvableCredentialLeavesCloudLimitsToTheVend() {
    StorageLocationValidator validator =
        new StorageLocationValidator(
            cloudCredentialVendor,
            (session, location) -> {
              throw new BaseException(
                  ErrorCode.INTERNAL, "The storage credential no longer exists");
            },
            () -> Unit.UTF16_CODE_UNITS);
    NormalizedURL root = NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(600));

    assertThat(rootRejection(validator, root, MANAGED_STORAGE_CATALOG_PREFIX)).isEmpty();
    assertThatCode(
            () ->
                validator.validateLocation(
                    null,
                    NormalizedURL.from("file:///tmp/" + "f".repeat(600)),
                    2048,
                    Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    // With the credential resolved, the same root is too long.
    assertThat(rootRejection(validatorFor(Optional.empty()), root, MANAGED_STORAGE_CATALOG_PREFIX))
        .isPresent();
  }

  @Test
  public void testExternalLocationWithForceDeletedCredentialIsLimitedOnlyByTheColumn() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    // The server assumes its own role for this bucket, and the read policy of this URL is too long.
    NormalizedURL url = NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(600));
    CredentialDAO awsIamRole =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("role-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN)),
            "test-user",
            SECRET_STORE);

    assertThatCode(() -> validator.validateExternalLocationUrl(url, 2048, Optional.empty()))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () -> validator.validateExternalLocationUrl(url, 2048, Optional.of(awsIamRole)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("its AWS STS session policy is");
    assertThatThrownBy(
            () ->
                validator.validateExternalLocationUrl(
                    NormalizedURL.from(STS_BUCKET + "/" + "r".repeat(2048)),
                    2048,
                    Optional.empty()))
        .isInstanceOf(BaseException.class)
        .hasMessageStartingWith("Storage location is too long: it is ");
  }

  @Test
  public void testGcsObjectNamesAreCountedInBytes() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    // An unconfigured bucket builds no access boundary, so only the object name limit applies.
    String bucket = "gs://unconfigured-bucket/";
    String threeByteCharacter = "資";
    int longestAccepted =
        IntStream.rangeClosed(1, 400)
            .filter(
                length -> {
                  try {
                    validator.validateLocation(
                        null,
                        NormalizedURL.from(bucket + threeByteCharacter.repeat(length)),
                        2048,
                        Extent.OBJECT_OR_PREFIX);
                    return true;
                  } catch (BaseException e) {
                    assertThat(e).hasMessageContaining("its GCS object name has");
                    return false;
                  }
                })
            .max()
            .getAsInt();
    // GCS: object names are "1-1024 bytes when UTF-8 encoded" (Objects, checked 2026-11-21).
    assertThat(3 * longestAccepted).isBetween(1022, 1024);
  }

  @Test
  public void testGcsConditionIsCheckedOnlyWhenTheVendBuildsOne() {
    StorageLocationValidator validator = validatorFor(Optional.empty());
    String path = "/" + "g".repeat(500);

    assertThatThrownBy(
            () ->
                validator.validateLocation(
                    null, NormalizedURL.from(GCS_BUCKET + path), 2048, Extent.OBJECT_OR_PREFIX))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("its GCS access boundary condition is");
    assertThatCode(
            () ->
                validator.validateLocation(
                    null,
                    NormalizedURL.from(CUSTOM_GCS_BUCKET + path),
                    2048,
                    Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    // A vend for a location with a storage credential builds no access boundary.
    CredentialDAO credential =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("role-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN)),
            "test-user",
            SECRET_STORE);
    assertThatCode(
            () ->
                validatorFor(Optional.of(credential))
                    .validateLocation(
                        null, NormalizedURL.from(GCS_BUCKET + path), 2048, Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                validator.validateLocation(
                    null,
                    NormalizedURL.from("gs://unconfigured-bucket" + path),
                    2048,
                    Extent.OBJECT_OR_PREFIX))
        .doesNotThrowAnyException();
  }
}
