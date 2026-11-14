package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.S3AssumeRole;
import io.unitycatalog.server.model.S3CompatibleRequest;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.secret.EncryptedCredentialSecretStore;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3BucketRegions;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcsStorageConfig;
import io.unitycatalog.server.service.credential.gcp.StaticTestingCredentialGenerator;
import io.unitycatalog.server.service.credential.gcp.TestingCredentialGenerator;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.S3CompatibleStaticKeyVending;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CloudCredentialVendorTest {
  private static final Instant SESSION_EXPIRATION = Instant.now().plus(Duration.ofHours(1));

  @Mock ServerProperties serverProperties;
  @Mock ExternalLocationUtils externalLocationUtils;
  @Mock S3BucketRegions s3BucketRegions;
  CloudCredentialVendor credentialsOperations;

  // The region S3-compatible storage reports for its bucket when the location configures none.
  private static final String DISCOVERED_REGION = "discovered-region-1";

  private static final EncryptedCredentialSecretStore SECRET_STORE =
      new EncryptedCredentialSecretStore(Optional.of(TestUtils.CREDENTIAL_ENCRYPTION_KEY));

  @BeforeEach
  public void setUp() {
    lenient()
        .when(serverProperties.get(ServerProperties.Property.CREDENTIAL_ENCRYPTION_KEY))
        .thenReturn(TestUtils.CREDENTIAL_ENCRYPTION_KEY);
    // The key is read only with s3-compatible storage enabled.
    lenient().when(serverProperties.isS3CompatibleStorageEnabled()).thenReturn(true);
    // Assumes no external location (or credential)
    doReturn(Optional.empty())
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());
    lenient()
        .when(s3BucketRegions.getS3CompatibleRegion(any(), any()))
        .thenReturn(DISCOVERED_REGION);
  }

  private TemporaryCredentials vendCredential(
      String path, Set<CredentialContext.Privilege> privileges) {
    StorageCredentialVendor storageCredentialVendor =
        new StorageCredentialVendorImpl(
            credentialsOperations, externalLocationUtils, serverProperties, s3BucketRegions);
    return storageCredentialVendor.vendCredential(
        NormalizedURL.from(path), privileges, CredentialUse.CLIENT);
  }

  @Test
  public void testRejectsCloudStorageRootBeforeVendingCredentials() {
    reset(externalLocationUtils);
    credentialsOperations = new CloudCredentialVendor(null, null, null);

    assertThatThrownBy(
            () -> vendCredential("s3://storageBase/", Set.of(CredentialContext.Privilege.SELECT)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("must include a non-empty path prefix")
        .hasMessageContaining("s3://storageBase")
        .extracting(exception -> ((BaseException) exception).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  @Test
  public void testGenerateS3TemporaryCredentials() {
    final String ACCESS_KEY = "accessKey";
    final String SECRET_KEY = "secretKey";
    final String SESSION_TOKEN = "sessionToken";
    final String S3_REGION = "us-west-2";
    final String ROLE_ARN = "roleArn";
    // Test session key is available
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("s3://storageBase"),
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .sessionToken(SESSION_TOKEN)
                    .build()));
    AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);
    TemporaryCredentials s3TemporaryCredentials =
        vendCredential("s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(s3TemporaryCredentials.getAwsTempCredentials())
        .isEqualTo(
            new AwsCredentials()
                .accessKeyId(ACCESS_KEY)
                .secretAccessKey(SECRET_KEY)
                .sessionToken(SESSION_TOKEN));
    // The vended url is the normalized path, not the raw request: a trailing slash is stripped.
    // This fails if the vendor ever echoes the caller-supplied string verbatim.
    assertThat(
            vendCredential("s3://storageBase/abc/", Set.of(CredentialContext.Privilege.SELECT))
                .getUrl())
        .isEqualTo("s3://storageBase/abc");

    // Test when sts client is called
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("s3://storageBase"),
                S3StorageConfig.builder()
                    .accessKey(ACCESS_KEY)
                    .secretKey(SECRET_KEY)
                    .region(S3_REGION)
                    .awsRoleArn(ROLE_ARN)
                    .build()));
    // A mocked STS client: an error response from STS, and a failure to reach it. Either is the
    // cause of a general error, which the exception handler logs.
    for (SdkException failure :
        List.<SdkException>of(
            StsException.builder().message("not authorized").statusCode(403).build(),
            SdkClientException.create("Unable to connect to sts.us-west-2.amazonaws.com"))) {
      StsClient stsClient = Mockito.mock(StsClient.class);
      when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenThrow(failure);
      StsClientBuilder stsClientBuilder = Mockito.mock(StsClientBuilder.class);
      when(stsClientBuilder.region(any())).thenReturn(stsClientBuilder);
      when(stsClientBuilder.credentialsProvider(any())).thenReturn(stsClientBuilder);
      when(stsClientBuilder.build()).thenReturn(stsClient);
      credentialsOperations =
          new CloudCredentialVendor(
              new AwsCredentialVendor(serverProperties, () -> stsClientBuilder), null, null);
      assertThatThrownBy(
              () ->
                  vendCredential(
                      "s3://storageBase/abc", Set.of(CredentialContext.Privilege.SELECT)))
          .as(failure.getClass().getSimpleName())
          .isInstanceOf(BaseException.class)
          .hasMessage("Failed to vend temporary storage credentials.")
          .hasCause(failure)
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.INTERNAL);
    }
  }

  @Test
  public void testGenerateAzureTemporaryCredentials() {
    final String CLIENT_ID = "clientId";
    final String CLIENT_SECRET = "clientSecret";
    final String TENANT_ID = "tenantId";
    // Test mode used
    when(serverProperties.getAdlsConfigurations())
        .thenReturn(Map.of("uctest", ADLSStorageConfig.builder().testMode(true).build()));
    AzureCredentialVendor azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, azureCredentialVendor, null);
    TemporaryCredentials azureTemporaryCredentials =
        vendCredential(
            "abfss://test@uctest.dfs.core.windows.net/abc",
            Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(azureTemporaryCredentials.getAzureUserDelegationSas().getSasToken()).isNotNull();
    assertThatThrownBy(
            () ->
                vendCredential(
                    "abfss://test@unconfigured.dfs.core.windows.net/abc",
                    Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Azure storage account configuration not found")
        .extracting(exception -> ((BaseException) exception).getErrorCode())
        .isEqualTo(ErrorCode.FAILED_PRECONDITION);

    // Use datalake service client
    when(serverProperties.getAdlsConfigurations())
        .thenReturn(
            Map.of(
                "uctest",
                ADLSStorageConfig.builder()
                    .testMode(false)
                    .tenantId(TENANT_ID)
                    .clientId(CLIENT_ID)
                    .clientSecret(CLIENT_SECRET)
                    .build()));
    azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, azureCredentialVendor, null);
    assertThatThrownBy(
            () ->
                vendCredential(
                    "abfss://test@uctest/abc", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(CompletionException.class);
  }

  @Test
  public void testGenerateGcpTemporaryCredentials() {
    // Test mode using a static generator supplied by the test suite
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath("")
                    .credentialGenerator(StaticTestingCredentialGenerator.class.getName())
                    .build()));
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    TemporaryCredentials gcpTemporaryCredentials =
        vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
    assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

    // Testing shortcut using the legacy json key sentinel.
    final String testingSentinel = "testing://sentinel";
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder()
                    .bucketPath("gs://uctest")
                    .jsonKeyFilePath(testingSentinel)
                    .credentialGenerator(TestingCredentialGenerator.class.getName())
                    .build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    TemporaryCredentials testingSentinelCredentials =
        vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.SELECT));
    assertThat(testingSentinelCredentials.getGcpOauthToken().getOauthToken())
        .isEqualTo(testingSentinel);

    // Use default creds (expected to fail without real GCP credentials)
    when(serverProperties.getGcsConfigurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("gs://uctest"),
                GcsStorageConfig.builder().bucketPath("gs://uctest").build()));
    gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () -> vendCredential("gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class);
  }

  @Test
  public void testMissingGcpBucketConfigurationFails() {
    when(serverProperties.getGcsConfigurations()).thenReturn(Map.of());
    GcpCredentialVendor gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    credentialsOperations = new CloudCredentialVendor(null, null, gcpCredentialVendor);
    assertThatThrownBy(
            () -> vendCredential("gs://missing/abc", Set.of(CredentialContext.Privilege.UPDATE)))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Unknown GCS storage configuration");
  }

  @Test
  public void testVendCredentialWithExternalLocationCredential() {
    final String CREDENTIAL_ROLE_ARN = "arn:aws:iam::123456789012:role/external-location-role";
    final String S3_PATH = "s3://my-bucket/path/to/data";
    final String VENDED_ACCESS_KEY = "vendedAccessKey";
    final String VENDED_SECRET_KEY = "vendedSecretKey";
    final String VENDED_SESSION_TOKEN = "vendedSessionToken";

    // Create a CredentialDAO representing the credential associated with an external location
    CredentialDAO credentialDAO =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("test-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(CREDENTIAL_ROLE_ARN)),
            "test-user",
            SECRET_STORE);
    String expectedExternalId = credentialDAO.getAwsIamRoleResponse().getExternalId();

    // Reset the mock to override the default stub from setUp()
    reset(externalLocationUtils);
    // Mock the external location utils to return this credential for the path
    doReturn(Optional.of(credentialDAO))
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());

    // No per-bucket configurations needed when using credential from external location
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    // Return empty master config
    doReturn(S3StorageConfig.builder().build())
        .when(serverProperties)
        .getS3MasterRoleConfiguration();

    // Create a mock StsClient that captures the AssumeRoleRequest
    StsClient mockStsClient = Mockito.mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Credentials stsCredentials =
        Credentials.builder()
            .accessKeyId(VENDED_ACCESS_KEY)
            .secretAccessKey(VENDED_SECRET_KEY)
            .sessionToken(VENDED_SESSION_TOKEN)
            .expiration(SESSION_EXPIRATION)
            .build();
    when(mockStsClient.assumeRole(requestCaptor.capture()))
        .thenReturn(AssumeRoleResponse.builder().credentials(stsCredentials).build());

    // Mock StsClient.builder() to return our mock StsClient
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);

      // Create the AwsCredentialVendor - when it lazily initializes the master role generator,
      // it will use the mocked StsClient.builder()
      AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
      credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);

      // Vend credentials - this should use the master role generator path
      TemporaryCredentials credentials =
          vendCredential(S3_PATH, Set.of(CredentialContext.Privilege.SELECT));

      // Verify the returned credentials match what the mock STS client returned
      assertThat(credentials.getAwsTempCredentials())
          .isEqualTo(
              new AwsCredentials()
                  .accessKeyId(VENDED_ACCESS_KEY)
                  .secretAccessKey(VENDED_SECRET_KEY)
                  .sessionToken(VENDED_SESSION_TOKEN));

      // Verify StsClient.assumeRole was called
      verify(mockStsClient).assumeRole(any(AssumeRoleRequest.class));

      // Verify the AssumeRoleRequest contains the correct roleArn and externalId
      // from the CredentialDAO associated with the external location
      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      assertThat(capturedRequest.roleArn()).isEqualTo(CREDENTIAL_ROLE_ARN);
      assertThat(capturedRequest.externalId()).isEqualTo(expectedExternalId);
    }
  }

  private static final String S3_ENDPOINT = "http://storage.example.com:9000";
  private static final String COMPAT_PATH = "s3://compat-bucket/data";

  /** Points every path at an external location on S3-compatible storage with this key pair. */
  private CredentialDAO useS3Compatible(S3CompatibleRequest s3Compatible) {
    s3Compatible.s3Endpoint(S3_ENDPOINT);
    // A spy, so tests can check when the secret is loaded.
    CredentialDAO credentialDAO =
        spy(
            CredentialDAO.from(
                new CreateCredentialRequest()
                    .name("compat-credential")
                    .purpose(CredentialPurpose.STORAGE)
                    .s3Compatible(s3Compatible),
                "test-user",
                SECRET_STORE));
    reset(externalLocationUtils);
    doReturn(Optional.of(credentialDAO))
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    // The master AWS identity and region must not be used for S3-compatible storage.
    doReturn(
            S3StorageConfig.builder()
                .region("us-west-2")
                .accessKey("master-key-id")
                .secretKey("master-key-value")
                .build())
        .when(serverProperties)
        .getS3MasterRoleConfiguration();
    return credentialDAO;
  }

  // AssumeRole goes to the STS endpoint, or to the S3 endpoint when the credential has none.
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "http://sts.storage.example.com:9000")
  public void testS3CompatibleAssumesRoleOnItsOwnStsEndpoint(String stsEndpoint) {
    final String ROLE_ARN = "arn:aws:iam::123456789012:role/compat-role";
    useS3Compatible(
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .assumeRole(new S3AssumeRole().stsEndpoint(stsEndpoint).roleArn(ROLE_ARN)));

    StsClient mockStsClient = Mockito.mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    when(mockStsClient.assumeRole(requestCaptor.capture()))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("vended-id")
                        .secretAccessKey("vended-value")
                        .sessionToken("vended-session")
                        .expiration(SESSION_EXPIRATION)
                        .build())
                .build());
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);
    ArgumentCaptor<AwsCredentialsProvider> providerCaptor =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);

    // A credential with assume_role always vends sessions, whatever the mode for key pairs.
    when(serverProperties.getS3CompatibleStaticKeyVending())
        .thenReturn(S3CompatibleStaticKeyVending.STATIC_KEY);
    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);
      credentialsOperations =
          new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);
      TemporaryCredentials credentials =
          vendCredential(COMPAT_PATH, Set.of(CredentialContext.Privilege.SELECT));

      assertThat(credentials.getAwsTempCredentials())
          .isEqualTo(
              new AwsCredentials()
                  .accessKeyId("vended-id")
                  .secretAccessKey("vended-value")
                  .sessionToken("vended-session")
                  .s3Endpoint(S3_ENDPOINT)
                  .s3Region(DISCOVERED_REGION));
      assertThat(credentials.getUrl()).isEqualTo(COMPAT_PATH);
    }
    assertThat(requestCaptor.getValue().roleArn()).isEqualTo(ROLE_ARN);
    assertThat(requestCaptor.getValue().policy()).contains("arn:aws:s3:::compat-bucket/data");
    verify(mockBuilder)
        .endpointOverride(URI.create(stsEndpoint == null ? S3_ENDPOINT : stsEndpoint));
    // STS runs on the storage, so AssumeRole is signed with the storage's region.
    verify(mockBuilder).region(Region.of(DISCOVERED_REGION));
    // The client holds no keys; the request is signed with the credential's key pair.
    verify(mockBuilder).credentialsProvider(providerCaptor.capture());
    assertThat(providerCaptor.getValue()).isInstanceOf(AnonymousCredentialsProvider.class);
    assertThat(signingKeyId(requestCaptor.getValue())).isEqualTo("key-id-1");
    assertThat(signer(requestCaptor.getValue()).resolveCredentials().secretAccessKey())
        .isEqualTo("key-value-1");
    // Closing the vendor, as the server does on shutdown, closes its STS client.
    verify(mockStsClient, never()).close();
    credentialsOperations.close();
    verify(mockStsClient).close();
  }

  private static AwsCredentialsProvider signer(AssumeRoleRequest request) {
    return request
        .overrideConfiguration()
        .flatMap(AwsRequestOverrideConfiguration::credentialsProvider)
        .orElseThrow();
  }

  private static String signingKeyId(AssumeRoleRequest request) {
    return signer(request).resolveCredentials().accessKeyId();
  }

  private record RegionCase(String stsRegion, String expectedStsRegion) {}

  @Test
  public void testS3CompatibleStsRegionResolvesFromConfigurationThenStorage() {
    List<RegionCase> cases =
        List.of(
            // Nothing configured: AssumeRole is signed with the region the storage reports.
            new RegionCase(null, DISCOVERED_REGION),
            // An STS region applies to AssumeRole only; S3 keeps the reported region.
            new RegionCase("sts-region-1", "sts-region-1"));
    for (RegionCase c : cases) {
      useS3Compatible(
          new S3CompatibleRequest()
              .accessKeyId("key-id-1")
              .secretAccessKey("key-value-1")
              .assumeRole(
                  new S3AssumeRole()
                      .stsEndpoint("http://storage.example.com:9000")
                      .roleArn("arn:aws:iam::123456789012:role/compat-role")
                      .stsRegion(c.stsRegion())));
      StsClient mockStsClient = Mockito.mock(StsClient.class);
      when(mockStsClient.assumeRole(any(AssumeRoleRequest.class)))
          .thenReturn(
              AssumeRoleResponse.builder()
                  .credentials(
                      Credentials.builder()
                          .accessKeyId("vended-id")
                          .secretAccessKey("vended-value")
                          .sessionToken("vended-session")
                          .expiration(SESSION_EXPIRATION)
                          .build())
                  .build());
      StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
      when(mockBuilder.region(any())).thenReturn(mockBuilder);
      when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
      when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockStsClient);

      try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
        mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);
        credentialsOperations =
            new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);
        TemporaryCredentials credentials = vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);

        assertThat(credentials.getAwsTempCredentials().getS3Region())
            .as(c.toString())
            .isEqualTo(DISCOVERED_REGION);
      }
      verify(mockBuilder).region(Region.of(c.expectedStsRegion()));
    }
  }

  @Test
  public void testS3CompatibleStsGeneratorPerSigningRegion() {
    // One credential used for storage in two regions gets an STS client for each.
    useS3Compatible(
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .assumeRole(
                new S3AssumeRole()
                    .stsEndpoint("http://storage.example.com:9000")
                    .roleArn("arn:aws:iam::123456789012:role/compat-role")));
    StsClient mockStsClient = Mockito.mock(StsClient.class);
    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("vended-id")
                        .secretAccessKey("v")
                        .sessionToken("t")
                        .expiration(SESSION_EXPIRATION)
                        .build())
                .build());
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);
    when(s3BucketRegions.getS3CompatibleRegion(any(), any()))
        .thenReturn("region-a", "region-b", "region-a");

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);
      credentialsOperations =
          new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);
      for (int i = 0; i < 3; i++) {
        vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);
      }
    }
    verify(mockBuilder).region(Region.of("region-a"));
    verify(mockBuilder).region(Region.of("region-b"));
    verify(mockBuilder, times(2)).build();
  }

  @Test
  public void testS3CompatibleSignsWithTheCredentialVersionItRead() {
    S3AssumeRole assumeRole =
        new S3AssumeRole()
            .stsEndpoint("http://storage.example.com:9000")
            .roleArn("arn:aws:iam::123456789012:role/compat-role");
    CredentialDAO credentialDAO =
        useS3Compatible(
            new S3CompatibleRequest()
                .accessKeyId("key-id-1")
                .secretAccessKey("key-value-1")
                .assumeRole(assumeRole));
    // The same credential as a request read it before the updates below.
    S3CompatibleRequest firstVersion =
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .s3Endpoint(S3_ENDPOINT)
            .assumeRole(assumeRole);
    CredentialDAO earlierRead =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("compat-credential")
                .purpose(CredentialPurpose.STORAGE)
                .s3Compatible(firstVersion),
            "test-user",
            SECRET_STORE);
    // Stored again under the credential's id, which the stored value is bound to.
    earlierRead.setId(credentialDAO.getId());
    earlierRead.setS3Compatible(firstVersion, SECRET_STORE);
    List<StsClient> clients = List.of(Mockito.mock(StsClient.class), Mockito.mock(StsClient.class));
    List<AssumeRoleRequest> requests = new ArrayList<>();
    for (StsClient client : clients) {
      when(client.assumeRole(any(AssumeRoleRequest.class)))
          .thenAnswer(
              invocation -> {
                requests.add(invocation.getArgument(0));
                return AssumeRoleResponse.builder()
                    .credentials(
                        Credentials.builder()
                            .accessKeyId("vended-id")
                            .secretAccessKey("v")
                            .sessionToken("t")
                            .expiration(SESSION_EXPIRATION)
                            .build())
                    .build();
              });
    }
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(clients.get(0), clients.get(1));

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);
      credentialsOperations =
          new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);
      vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);

      // A new key pair needs no new client: the request carries it.
      credentialDAO.setS3Compatible(
          new S3CompatibleRequest()
              .accessKeyId("key-id-2")
              .secretAccessKey("key-value-2")
              .s3Endpoint(S3_ENDPOINT)
              .assumeRole(assumeRole),
          SECRET_STORE);
      vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);

      // A request that read the credential before that update signs with the key pair it read.
      doReturn(Optional.of(earlierRead))
          .when(externalLocationUtils)
          .getExternalLocationCredentialDaoForPath(any());
      vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);

      // Another STS endpoint gets its own client, and the request goes there with its own keys.
      credentialDAO.setS3Compatible(
          new S3CompatibleRequest()
              .accessKeyId("key-id-3")
              .secretAccessKey("key-value-3")
              .s3Endpoint(S3_ENDPOINT)
              .assumeRole(
                  new S3AssumeRole()
                      .stsEndpoint("http://sts.other.example.com:9000")
                      .roleArn(assumeRole.getRoleArn())),
          SECRET_STORE);
      doReturn(Optional.of(credentialDAO))
          .when(externalLocationUtils)
          .getExternalLocationCredentialDaoForPath(any());
      vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY);
    }
    assertThat(requests.stream().map(CloudCredentialVendorTest::signingKeyId))
        .containsExactly("key-id-1", "key-id-2", "key-id-1", "key-id-3");
    verify(mockBuilder, times(2)).build();
    verify(mockBuilder).endpointOverride(URI.create("http://storage.example.com:9000"));
    verify(mockBuilder).endpointOverride(URI.create("http://sts.other.example.com:9000"));
    verify(clients.get(0), times(3)).assumeRole(any(AssumeRoleRequest.class));
    verify(clients.get(1)).assumeRole(any(AssumeRoleRequest.class));
    verify(clients.get(0), never()).close();
  }

  @Test
  public void testS3CompatibleStsFailureReturnsGeneralError() {
    useS3Compatible(
        new S3CompatibleRequest()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .assumeRole(
                new S3AssumeRole()
                    .stsEndpoint("http://sts.storage.example.com:9000")
                    .roleArn("arn:aws:iam::123456789012:role/compat-role")));
    StsClient mockStsClient = Mockito.mock(StsClient.class);
    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenThrow(
            StsException.builder()
                .message("invalid condition key 'kms:ViaService'")
                .statusCode(400)
                .build())
        // An STS other than AWS may answer without credentials, or with an empty element.
        .thenReturn(AssumeRoleResponse.builder().build())
        .thenReturn(AssumeRoleResponse.builder().credentials(Credentials.builder().build()).build())
        // A session without an expiration would never be renewed, and a blank key is no key.
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("id")
                        .secretAccessKey("v")
                        .sessionToken("t")
                        .build())
                .build())
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(" ")
                        .secretAccessKey("v")
                        .sessionToken("t")
                        .expiration(SESSION_EXPIRATION)
                        .build())
                .build());
    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);
      credentialsOperations =
          new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);

      // The storage's response text stays in the server log.
      assertThatThrownBy(
              () -> vendCredential(COMPAT_PATH, Set.of(CredentialContext.Privilege.SELECT)))
          .isInstanceOf(BaseException.class)
          .hasMessage("Failed to vend temporary storage credentials.")
          .hasCauseInstanceOf(StsException.class);
      for (int i = 0; i < 4; i++) {
        assertThatThrownBy(
                () -> vendCredential(COMPAT_PATH, Set.of(CredentialContext.Privilege.SELECT)))
            .isInstanceOf(BaseException.class)
            .hasMessage("Failed to vend temporary storage credentials.");
      }
    }
  }

  @Test
  public void testUnreadableSecretReturnsGeneralError() {
    // The reason names the server's key, so the caller gets the general error; the server logs it.
    useS3Compatible(
        new S3CompatibleRequest().accessKeyId("key-id-1").secretAccessKey("key-value-1"));
    when(serverProperties.getS3CompatibleStaticKeyVending())
        .thenReturn(S3CompatibleStaticKeyVending.STATIC_KEY);
    when(serverProperties.get(ServerProperties.Property.CREDENTIAL_ENCRYPTION_KEY))
        .thenReturn(Base64.getEncoder().encodeToString(new byte[32]));
    credentialsOperations =
        new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);

    assertThatThrownBy(() -> vendCredential(COMPAT_PATH, CredentialContext.READ_ONLY))
        .isInstanceOf(BaseException.class)
        .hasMessage("Failed to vend temporary storage credentials.")
        .cause()
        .hasMessageContaining("encrypted with key");
  }

  @Test
  public void testS3CompatibleIsNotVendedWhenS3CompatibleStorageIsDisabled() {
    // The flag is off, as by default.
    doCallRealMethod().when(serverProperties).checkS3CompatibleStorageEnabled();
    doCallRealMethod().when(serverProperties).isS3CompatibleStorageEnabled();
    useS3Compatible(
        new S3CompatibleRequest().accessKeyId("key-id-1").secretAccessKey("key-value-1"));
    StorageCredentialVendor vendor =
        new StorageCredentialVendorImpl(
            new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null),
            externalLocationUtils,
            serverProperties,
            s3BucketRegions);

    assertThatThrownBy(
            () ->
                vendor.vendCredential(
                    NormalizedURL.from(COMPAT_PATH),
                    CredentialContext.READ_ONLY,
                    CredentialUse.CLIENT))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("server.s3-compatible-storage.enabled=true");
    // Rejected before the storage is asked for the bucket's region.
    verify(s3BucketRegions, never()).getS3CompatibleRegion(any(), any());
  }

  private record StaticKeyCase(
      S3CompatibleStaticKeyVending mode, boolean isServerAccess, AwsCredentials expected) {}

  @Test
  public void testS3CompatibleWithoutAssumeRoleVendsPerServerMode() {
    AwsCredentials keyPair =
        new AwsCredentials()
            .accessKeyId("key-id-1")
            .secretAccessKey("key-value-1")
            .s3Endpoint(S3_ENDPOINT)
            .s3Region(DISCOVERED_REGION);
    List<StaticKeyCase> cases =
        List.of(
            // Clients get only the endpoint and region, and use their own keys.
            new StaticKeyCase(
                S3CompatibleStaticKeyVending.ENDPOINT_ONLY,
                /* isServerAccess= */ false,
                new AwsCredentials().s3Endpoint(S3_ENDPOINT).s3Region(DISCOVERED_REGION)),
            new StaticKeyCase(
                S3CompatibleStaticKeyVending.STATIC_KEY, /* isServerAccess= */ false, keyPair),
            // The server always needs usable keys for its own storage access.
            new StaticKeyCase(
                S3CompatibleStaticKeyVending.ENDPOINT_ONLY, /* isServerAccess= */ true, keyPair),
            new StaticKeyCase(
                S3CompatibleStaticKeyVending.STATIC_KEY, /* isServerAccess= */ true, keyPair));
    for (StaticKeyCase c : cases) {
      when(serverProperties.getS3CompatibleStaticKeyVending()).thenReturn(c.mode());
      CredentialDAO credentialDAO =
          useS3Compatible(
              new S3CompatibleRequest().accessKeyId("key-id-1").secretAccessKey("key-value-1"));
      credentialsOperations =
          new CloudCredentialVendor(new AwsCredentialVendor(serverProperties), null, null);
      StorageCredentialVendor vendor =
          new StorageCredentialVendorImpl(
              credentialsOperations, externalLocationUtils, serverProperties, s3BucketRegions);
      long before = System.currentTimeMillis();
      TemporaryCredentials credentials =
          vendor.vendCredential(
              NormalizedURL.from(COMPAT_PATH),
              CredentialContext.READ_ONLY,
              c.isServerAccess() ? CredentialUse.SERVER : CredentialUse.CLIENT);

      assertThat(credentials.getAwsTempCredentials()).as(c.toString()).isEqualTo(c.expected());
      // Only a vend that returns the key pair loads the secret from its store.
      verify(credentialDAO, times(c.expected().getSecretAccessKey() == null ? 0 : 1))
          .getS3SecretAccessKey(any());
      if (!c.isServerAccess()) {
        // What clients get, the key pair or only the endpoint, carries the lifetime of an STS
        // session, so clients vend again and pick up an updated key pair or endpoint.
        long lifetime = Duration.ofHours(1).toMillis();
        assertThat(credentials.getExpirationTime())
            .as(c.toString())
            .isBetween(before + lifetime, System.currentTimeMillis() + lifetime);
      } else {
        assertThat(credentials.getExpirationTime()).as(c.toString()).isNull();
      }
    }
  }

  @Test
  public void testGovCloudRoleArnUsesAwsUsGovS3ArnsInSessionPolicy() throws Exception {
    final String CREDENTIAL_ROLE_ARN =
        "arn:aws-us-gov:iam::123456789012:role/external-location-role";
    final String S3_PATH = "s3://gov-bucket/path/to/data";

    CredentialDAO credentialDAO =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("test-credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(CREDENTIAL_ROLE_ARN)),
            "test-user",
            SECRET_STORE);

    reset(externalLocationUtils);
    doReturn(Optional.of(credentialDAO))
        .when(externalLocationUtils)
        .getExternalLocationCredentialDaoForPath(any());

    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
    // Master region intentionally commercial: the assumed role ARN must still select aws-us-gov.
    doReturn(S3StorageConfig.builder().region("us-east-1").build())
        .when(serverProperties)
        .getS3MasterRoleConfiguration();

    StsClient mockStsClient = Mockito.mock(StsClient.class);
    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Credentials stsCredentials =
        Credentials.builder()
            .accessKeyId("vendedAccessKey")
            .secretAccessKey("vendedSecretKey")
            .sessionToken("vendedSessionToken")
            .expiration(SESSION_EXPIRATION)
            .build();
    when(mockStsClient.assumeRole(requestCaptor.capture()))
        .thenReturn(AssumeRoleResponse.builder().credentials(stsCredentials).build());

    StsClientBuilder mockBuilder = Mockito.mock(StsClientBuilder.class);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> mockedStsClient = Mockito.mockStatic(StsClient.class)) {
      mockedStsClient.when(StsClient::builder).thenReturn(mockBuilder);

      AwsCredentialVendor awsCredentialVendor = new AwsCredentialVendor(serverProperties);
      credentialsOperations = new CloudCredentialVendor(awsCredentialVendor, null, null);

      vendCredential(S3_PATH, Set.of(CredentialContext.Privilege.SELECT));

      AssumeRoleRequest capturedRequest = requestCaptor.getValue();
      assertThat(capturedRequest.policy())
          .contains("arn:aws-us-gov:s3:::gov-bucket")
          .doesNotContain("arn:aws:s3:::gov-bucket");
      assertThat(
              new ObjectMapper()
                  .readTree(capturedRequest.policy())
                  .findPath("kms:ViaService")
                  .asText())
          .isEqualTo("s3.*.amazonaws.com");
    }
  }
}
