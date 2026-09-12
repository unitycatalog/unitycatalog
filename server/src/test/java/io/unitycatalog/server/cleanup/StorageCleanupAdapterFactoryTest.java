package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

class StorageCleanupAdapterFactoryTest {
  private static final UUID RESOURCE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID WRONG_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private StorageCredentialVendor credentialVendor;
  private ServerProperties serverProperties;

  @BeforeEach
  void setUp() {
    credentialVendor = mock(StorageCredentialVendor.class);
    serverProperties = mock(ServerProperties.class);
    when(serverProperties.getS3Configurations()).thenReturn(Map.of());
  }

  @ParameterizedTest
  @MethodSource("resourcePaths")
  void validatesEveryResourceIdentityBeforeVending(ResourceType resourceType, String segment) {
    StorageCleanupAdapterFactory factory = factory();
    String validPath = "/managed/" + segment + "/" + RESOURCE_ID;

    assertThat(factory.create(task(resourceType, validPath), TIMEOUT))
        .isInstanceOf(LocalStorageCleanupAdapter.class);
    for (String invalidPath :
        List.of(
            "s3://bucket/managed/" + segment + "/" + WRONG_ID,
            "s3://bucket/managed/wrong/" + RESOURCE_ID,
            "s3://bucket/managed/" + segment + "-other/" + RESOURCE_ID,
            "s3://bucket/managed/" + segment)) {
      assertThatThrownBy(() -> factory.create(task(resourceType, invalidPath), TIMEOUT))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup task location does not match its " + resourceType + " resource id");
    }
    verifyNoInteractions(credentialVendor);
  }

  @Test
  void fileAndPlainPathsNeverVendCredentials() {
    StorageCleanupAdapterFactory factory = factory();

    assertThat(
            factory.create(task(ResourceType.TABLE, "file:///tmp/tables/" + RESOURCE_ID), TIMEOUT))
        .isInstanceOf(LocalStorageCleanupAdapter.class);
    assertThat(factory.create(task(ResourceType.TABLE, "/tmp/tables/" + RESOURCE_ID), TIMEOUT))
        .isInstanceOf(LocalStorageCleanupAdapter.class);
    verifyNoInteractions(credentialVendor);
  }

  @Test
  void createsS3AdapterWithVendedSessionCredentialsAndConfiguredRegion() {
    NormalizedURL location = NormalizedURL.from("s3://bucket/managed/tables/" + RESOURCE_ID);
    when(serverProperties.getS3Configurations())
        .thenReturn(
            Map.of(
                NormalizedURL.from("s3://bucket"),
                S3StorageConfig.builder().region("us-west-2").build()));
    when(credentialVendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("access-key")
                        .secretAccessKey("secret-key")
                        .sessionToken("session-token")));
    S3Client client = mock(S3Client.class);
    S3ClientBuilder builder = mock(S3ClientBuilder.class);
    when(builder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(builder);
    when(builder.region(any(Region.class))).thenReturn(builder);
    when(builder.build()).thenReturn(client);
    AtomicReference<List<?>> constructorArgs = new AtomicReference<>();

    try (MockedStatic<S3Client> s3 = mockStatic(S3Client.class);
        MockedConstruction<S3StorageCleanupAdapter> adapters =
            Mockito.mockConstruction(
                S3StorageCleanupAdapter.class,
                (adapter, context) -> constructorArgs.set(context.arguments()))) {
      s3.when(S3Client::builder).thenReturn(builder);

      assertThat(factory().create(task(ResourceType.TABLE, location.toString()), TIMEOUT))
          .isSameAs(adapters.constructed().get(0));
    }

    verify(credentialVendor).vendCredential(location, CredentialContext.READ_WRITE);
    ArgumentCaptor<AwsCredentialsProvider> provider =
        ArgumentCaptor.forClass(AwsCredentialsProvider.class);
    verify(builder).credentialsProvider(provider.capture());
    software.amazon.awssdk.auth.credentials.AwsCredentials resolved =
        provider.getValue().resolveCredentials();
    assertThat(resolved)
        .isInstanceOf(AwsSessionCredentials.class)
        .extracting("accessKeyId", "secretAccessKey", "sessionToken")
        .containsExactly("access-key", "secret-key", "session-token");
    verify(builder).region(Region.US_WEST_2);
    assertThat(constructorArgs.get()).isEqualTo(List.of(client, location, TIMEOUT));
  }

  @Test
  void requiresConfiguredS3BucketRegion() {
    NormalizedURL location = NormalizedURL.from("s3://bucket/managed/tables/" + RESOURCE_ID);
    when(credentialVendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials().accessKeyId("key").secretAccessKey("secret")));

    try (MockedStatic<S3Client> s3 = mockStatic(S3Client.class)) {
      assertThatThrownBy(
              () -> factory().create(task(ResourceType.TABLE, location.toString()), TIMEOUT))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("S3 cleanup requires a configured region for the task bucket");
      s3.verifyNoInteractions();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs", "abfs", "abfss"})
  void rejectsUnsupportedCloudSchemesBeforeVendingCredentials(String scheme) {
    String location = scheme + "://bucket/managed/tables/" + RESOURCE_ID;

    assertThatThrownBy(() -> factory().create(task(ResourceType.TABLE, location), TIMEOUT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Storage cleanup supports only local files and S3");
    verifyNoInteractions(credentialVendor);
  }

  @Test
  void rejectsInvalidS3CredentialsWithoutExposingValues() {
    List<TemporaryCredentials> cases =
        List.of(
            new TemporaryCredentials(),
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials().accessKeyId("key").secretAccessKey("secret-value"))
                .gcpOauthToken(new GcpOauthToken().oauthToken("secret-value")),
            new TemporaryCredentials().awsTempCredentials(new AwsCredentials().accessKeyId("key")));

    try (MockedConstruction<S3StorageCleanupAdapter> s3Adapters =
        Mockito.mockConstruction(S3StorageCleanupAdapter.class)) {
      for (TemporaryCredentials credentials : cases) {
        when(credentialVendor.vendCredential(any(), any())).thenReturn(credentials);
        String location = "s3://bucket/managed/tables/" + RESOURCE_ID;
        assertThatThrownBy(() -> factory().create(task(ResourceType.TABLE, location), TIMEOUT))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Credential vendor returned invalid credentials")
            .hasMessageNotContaining("secret-value");
      }
      assertThat(s3Adapters.constructed()).isEmpty();
    }
  }

  private StorageCleanupAdapterFactory factory() {
    return new StorageCleanupAdapterFactory(credentialVendor, serverProperties);
  }

  private static StorageCleanupTaskDAO task(ResourceType resourceType, String location) {
    return StorageCleanupTaskDAO.builder()
        .resourceType(resourceType)
        .resourceId(RESOURCE_ID)
        .storageLocation(location)
        .build();
  }

  private static Stream<Arguments> resourcePaths() {
    return Stream.of(
        Arguments.of(ResourceType.TABLE, "tables"),
        Arguments.of(ResourceType.STAGING_TABLE, "tables"),
        Arguments.of(ResourceType.VOLUME, "volumes"),
        Arguments.of(ResourceType.REGISTERED_MODEL, "models"),
        Arguments.of(ResourceType.MODEL_VERSION, "versions"));
  }
}
