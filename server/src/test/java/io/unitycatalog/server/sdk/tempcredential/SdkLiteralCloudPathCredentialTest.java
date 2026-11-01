package io.unitycatalog.server.sdk.tempcredential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.BoundaryTestingCredentialGenerator;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class SdkLiteralCloudPathCredentialTest extends SdkAccessControlBaseCRUDTest {
  private static final String READER = "literal-path-reader@example.com";
  private static final String TABLE_NAME = "literal_path";

  private StsClient stsClient;
  private DataLakeServiceAsyncClient dataLakeClient;
  private UserDelegationKey delegationKey;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("s3.bucketPath.0", "s3://test-bucket");
    serverProperties.setProperty("s3.accessKey.0", "test-access-key");
    serverProperties.setProperty("s3.secretKey.0", "test-secret-key");
    serverProperties.setProperty("s3.region.0", "us-west-2");
    serverProperties.setProperty("s3.awsRoleArn.0", "arn:aws:iam::123456789012:role/test-storage");
    serverProperties.setProperty("gcs.bucketPath.0", "gs://test-bucket");
    serverProperties.setProperty(
        "gcs.credentialGenerator.0", BoundaryTestingCredentialGenerator.class.getName());
    serverProperties.setProperty("adls.storageAccountName.0", "testaccount");
    serverProperties.setProperty("adls.tenantId.0", "test-tenant");
    serverProperties.setProperty("adls.clientId.0", "test-client");
    serverProperties.setProperty("adls.clientSecret.0", "test-client-secret");
  }

  @Override
  protected void setUpCredentialOperations(ServerProperties properties) {
    stsClient = mock(StsClient.class);
    when(stsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("test-access-key")
                        .secretAccessKey("test-secret-key")
                        .sessionToken("test-session-token")
                        .build())
                .build());
    StsClientBuilder stsBuilder = mock(StsClientBuilder.class, RETURNS_SELF);
    when(stsBuilder.build()).thenReturn(stsClient);

    delegationKey =
        new UserDelegationKey()
            .setSignedObjectId("test-object")
            .setSignedTenantId("test-tenant")
            .setSignedStart(OffsetDateTime.parse("2026-01-01T00:00:00Z"))
            .setSignedExpiry(OffsetDateTime.parse("2030-01-01T00:00:00Z"))
            .setSignedService("b")
            .setSignedVersion("2025-05-05")
            .setValue(Base64.getEncoder().encodeToString(new byte[32]));
    dataLakeClient = mock(DataLakeServiceAsyncClient.class);
    when(dataLakeClient.getUserDelegationKey(any(), any())).thenReturn(Mono.just(delegationKey));

    AzureCredentialVendor azureVendor =
        new AzureCredentialVendor(properties) {
          @Override
          public AzureCredential vendAzureCredential(CredentialContext context) {
            // Construction mocking is thread-local, so install it on the HTTP request thread.
            try (MockedConstruction<DataLakeServiceClientBuilder> ignored =
                mockConstruction(
                    DataLakeServiceClientBuilder.class,
                    withSettings().defaultAnswer(RETURNS_SELF),
                    (builder, construction) ->
                        when(builder.buildAsyncClient()).thenReturn(dataLakeClient))) {
              return super.vendAzureCredential(context);
            }
          }
        };
    cloudCredentialVendor =
        new CloudCredentialVendor(
            new AwsCredentialVendor(properties, () -> stsBuilder),
            azureVendor,
            new GcpCredentialVendor(properties));
  }

  private static Stream<Arguments> literalLocations() {
    return Stream.of(
            "s3://test-bucket/team%20reports",
            "s3://test-bucket/%2F",
            "gs://test-bucket/team%20reports",
            "gs://test-bucket/%2F",
            "abfss://test-container@testaccount.dfs.core.windows.net/team%20reports",
            "abfss://test-container@testaccount.dfs.core.windows.net/%2F")
        .flatMap(location -> Stream.of(false, true).map(byPath -> Arguments.of(location, byPath)));
  }

  @ParameterizedTest
  @MethodSource("literalLocations")
  public void testCredentialScopeMatchesClientPath(String location, boolean byPath)
      throws Exception {
    TableInfo table =
        BaseTableCRUDTest.createTestingTable(
            TABLE_NAME,
            TableType.EXTERNAL,
            Optional.of(location),
            new SdkTableOperations(adminApiClient));
    assertThat(table.getStorageLocation()).isEqualTo(location);

    createTestUser(READER);
    grantPermissions(READER, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        READER, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    grantPermissions(
        READER,
        SecurableType.TABLE,
        TestUtils.SCHEMA_FULL_NAME + "." + TABLE_NAME,
        Privileges.SELECT);
    ApiClient readerClient = TestUtils.createApiClient(createTestUserServerConfig(READER));
    TemporaryCredentialsApi api = new TemporaryCredentialsApi(readerClient);
    TemporaryCredentials credential =
        byPath
            ? api.generateTemporaryPathCredentials(
                new GenerateTemporaryPathCredential()
                    .url(location)
                    .operation(PathOperation.PATH_READ))
            : api.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(table.getTableId())
                    .operation(TableOperation.READ));

    assertThat(credential.getUrl()).isEqualTo(location);
    Path clientPath = new Path(location);
    String prefix = clientPath.toUri().getPath().substring(1);
    switch (clientPath.toUri().getScheme()) {
      case "s3" -> {
        ArgumentCaptor<AssumeRoleRequest> request =
            ArgumentCaptor.forClass(AssumeRoleRequest.class);
        verify(stsClient).assumeRole(request.capture());
        JsonNode policy = new ObjectMapper().readTree(request.getValue().policy());
        assertThat(policy.get("Statement").get(0).get("Resource"))
            .map(JsonNode::asText)
            .containsExactly(
                "arn:aws:s3:::test-bucket/" + prefix + "/*", "arn:aws:s3:::test-bucket/" + prefix);
        assertThat(policy.get("Statement").get(1).findPath("s3:prefix"))
            .map(JsonNode::asText)
            .containsExactly(prefix, prefix + "/", prefix + "/*");
      }
      case "gs" ->
          assertThat(credential.getGcpOauthToken().getOauthToken())
              .isEqualTo(
                  "resource.name.startsWith(\"projects/_/buckets/test-bucket/objects/"
                      + prefix
                      + "\") || api.getAttribute('storage.googleapis.com/objectListPrefix', '')"
                      + ".startsWith(\""
                      + prefix
                      + "\")");
      case "abfss" -> {
        ArgumentCaptor<OffsetDateTime> start = ArgumentCaptor.forClass(OffsetDateTime.class);
        ArgumentCaptor<OffsetDateTime> expiry = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(dataLakeClient).getUserDelegationKey(start.capture(), expiry.capture());
        PathSasPermission permissions =
            new PathSasPermission().setReadPermission(true).setListPermission(true);
        String expectedSas =
            new DataLakeSasImplUtil(
                    new DataLakeServiceSasSignatureValues(expiry.getValue(), permissions)
                        .setStartTime(start.getValue()),
                    "test-container",
                    prefix,
                    true)
                .generateUserDelegationSas(delegationKey, "testaccount", Context.NONE);
        assertThat(credential.getAzureUserDelegationSas().getSasToken())
            .as("Directory SAS for the client's literal path %s", prefix)
            .isEqualTo(expectedSas);
      }
      default -> throw new IllegalArgumentException("Unexpected test scheme");
    }
  }
}
