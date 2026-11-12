package io.unitycatalog.server.service.sharing;

import static io.opensharing.auth.Privilege.USE_SHARE;
import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opensharing.auth.AuthContext;
import io.opensharing.auth.Privilege;
import io.opensharing.auth.UserContext;
import io.opensharing.catalog.Asset;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.CloudProvider;
import io.opensharing.catalog.CredentialRequest;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.catalog.StorageCredentials;
import io.opensharing.catalog.StorageOperation;
import io.opensharing.exception.AssetAccessDeniedException;
import io.opensharing.exception.CatalogAuthorizationException;
import io.opensharing.exception.CatalogException;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialUse;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * How the connector reports UC's failures, which a running server can't easily be made to hit, and
 * what it leaves to UC's own checks.
 */
public class UnityCatalogCatalogConnectorTest {

  private static final String USER_ID = "8c1e5c2a-3b4d-4f5e-9a6b-7c8d9e0f1a2b";
  private static final Asset ORDERS = new Asset(AssetType.TABLE, "main.sales.orders");
  private static final UUID CATALOG_ID = UUID.randomUUID();
  private static final UUID SCHEMA_ID = UUID.randomUUID();
  private static final UUID TABLE_ID = UUID.randomUUID();
  private static final Instant EXPIRATION = Instant.ofEpochMilli(1_800_000_000_000L);

  private final UserRepository users = mock(UserRepository.class);
  private final TableRepository tables = mock(TableRepository.class);
  private final MetastoreRepository metastore = mock(MetastoreRepository.class);
  private final KeyMapper keyMapper = mock(KeyMapper.class);
  private final UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
  // Mocked because the real vendor would call cloud STS, SAS and OAuth endpoints.
  private final StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
  private final Map<NormalizedURL, S3StorageConfig> s3Buckets = new HashMap<>();
  private final UnityCatalogCatalogConnector connector;

  public UnityCatalogCatalogConnectorTest() {
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getTableRepository()).thenReturn(tables);
    when(repositories.getMetastoreRepository()).thenReturn(metastore);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);
    when(repositories.getStorageCredentialVendor()).thenReturn(vendor);
    ServerProperties properties = mock(ServerProperties.class);
    when(properties.getS3Configurations()).thenReturn(s3Buckets);
    connector = new UnityCatalogCatalogConnector(repositories, authorizer, properties);
  }

  @Test
  public void authorizesAnEnabledUserWithTheMetastoreOwnersAdminFlag() {
    when(users.getUser(USER_ID))
        .thenReturn(
            new User().id(USER_ID).email("alice@example.com").state(User.StateEnum.ENABLED));
    UUID metastoreId = UUID.randomUUID();
    when(metastore.getMetastoreId()).thenReturn(metastoreId);
    for (Privilege privilege : Privilege.values()) {
      assertThat(connector.authorize(auth(), privilege).userId()).isEqualTo(USER_ID);
    }
    // Only the metastore OWNER may transfer the ownership of a share or recipient.
    assertThat(connector.authorize(auth(), USE_SHARE).admin()).isFalse();
    when(authorizer.authorize(UUID.fromString(USER_ID), METASTORE, metastoreId, Privileges.OWNER))
        .thenReturn(true);
    assertThat(connector.authorize(auth(), USE_SHARE).admin()).isTrue();
  }

  @Test
  public void unknownUserIsRefused() {
    when(users.getUser(USER_ID)).thenThrow(new BaseException(ErrorCode.NOT_FOUND, "not found"));
    assertThatThrownBy(() -> connector.authorize(auth(), null))
        .isInstanceOf(CatalogAuthorizationException.class);
  }

  @Test
  public void failedUserLookupIsCatalogFailure() {
    when(users.getUser(USER_ID)).thenThrow(new BaseException(ErrorCode.INTERNAL, "db is down"));
    assertThatThrownBy(() -> connector.authorize(auth(), null))
        .isExactlyInstanceOf(CatalogException.class)
        .hasCauseInstanceOf(BaseException.class);
  }

  @Test
  public void tableTheUserMayReadResolves() {
    ordersTable(/* maySelect= */ true);
    ResolvedAsset resolved = connector.resolveAsset(ORDERS, auth());
    assertThat(resolved.fullName()).isEqualTo(ORDERS.fullName());
    assertThat(resolved.catalogAssetId()).isEqualTo(TABLE_ID.toString());
    assertThat(resolved.location().storageLocation()).isEqualTo("s3://bucket/orders");
  }

  @Test
  public void tableTheUserMayNotReadIsDenied() {
    ordersTable(/* maySelect= */ false);
    assertThatThrownBy(() -> connector.resolveAsset(ORDERS, auth()))
        .isInstanceOf(AssetAccessDeniedException.class);
  }

  @Test
  public void malformedTableNameIsBadRequest() {
    assertThatThrownBy(
            () -> connector.resolveAsset(new Asset(AssetType.TABLE, "sales.orders"), auth()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void invalidDataInUnityCatalogIsCatalogFailure() {
    when(tables.getTable(ORDERS.fullName()))
        .thenThrow(new IllegalArgumentException("malformed storage location"));
    assertThatThrownBy(() -> connector.resolveAsset(ORDERS, auth()))
        .isExactlyInstanceOf(CatalogException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void credentialsAreVendedForReadOnly() {
    CredentialRequest notRead =
        new CredentialRequest(AssetType.TABLE, ORDERS.fullName(), null, null, null, null);
    assertThatThrownBy(() -> connector.getStorageCredentials(notRead, auth()))
        .isExactlyInstanceOf(CatalogException.class)
        .hasMessageContaining("read-only");
  }

  @Test
  public void awsCredentialsCarryTheKeysTokenAndConfiguredRegion() {
    ordersTable(/* maySelect= */ true, "s3://bucket/orders");
    s3Buckets.put(
        NormalizedURL.from("s3://bucket"), S3StorageConfig.builder().region("us-west-2").build());
    vend(
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA123")
                    .secretAccessKey("secret")
                    .sessionToken("token"))
            .expirationTime(EXPIRATION.toEpochMilli()));

    StorageCredentials vended = readCredentials();

    assertThat(vended.provider()).isEqualTo(CloudProvider.AWS);
    assertThat(vended.prefix()).isEqualTo("s3://bucket/orders");
    assertThat(vended.credentials())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                StorageCredentials.ACCESS_KEY_ID, "AKIA123",
                StorageCredentials.SECRET_ACCESS_KEY, "secret",
                StorageCredentials.SESSION_TOKEN, "token",
                StorageCredentials.REGION, "us-west-2"));
    assertThat(vended.expiration()).isEqualTo(EXPIRATION);
  }

  @Test
  public void awsCredentialsWithoutSessionTokenOrRegionOmitThem() {
    ordersTable(/* maySelect= */ true, "s3://bucket/orders");
    vend(
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("AKIA123").secretAccessKey("secret")));

    StorageCredentials vended = readCredentials();

    assertThat(vended.credentials())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                StorageCredentials.ACCESS_KEY_ID, "AKIA123",
                StorageCredentials.SECRET_ACCESS_KEY, "secret"));
    assertThat(vended.expiration()).isNull();
  }

  @Test
  public void azureCredentialsCarryTheSasToken() {
    String location = "abfss://container@acct.dfs.core.windows.net/orders";
    ordersTable(/* maySelect= */ true, location);
    vend(
        new TemporaryCredentials()
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sv=2024&sig=abc"))
            .expirationTime(EXPIRATION.toEpochMilli()));

    StorageCredentials vended = readCredentials();

    assertThat(vended.provider()).isEqualTo(CloudProvider.AZURE);
    assertThat(vended.prefix()).isEqualTo(location);
    assertThat(vended.credentials())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(StorageCredentials.SAS_TOKEN, "sv=2024&sig=abc"));
    assertThat(vended.expiration()).isEqualTo(EXPIRATION);
  }

  @Test
  public void gcpCredentialsCarryTheOauthToken() {
    ordersTable(/* maySelect= */ true, "gs://bucket/orders");
    vend(
        new TemporaryCredentials()
            .gcpOauthToken(new GcpOauthToken().oauthToken("ya29.token"))
            .expirationTime(EXPIRATION.toEpochMilli()));

    StorageCredentials vended = readCredentials();

    assertThat(vended.provider()).isEqualTo(CloudProvider.GCP);
    assertThat(vended.prefix()).isEqualTo("gs://bucket/orders");
    assertThat(vended.credentials())
        .containsExactlyInAnyOrderEntriesOf(Map.of(StorageCredentials.OAUTH_TOKEN, "ya29.token"));
    assertThat(vended.expiration()).isEqualTo(EXPIRATION);
  }

  @Test
  public void credentialsWithOnlyBlankValuesAreCatalogFailure() {
    ordersTable(/* maySelect= */ true, "gs://bucket/orders");
    vend(new TemporaryCredentials().gcpOauthToken(new GcpOauthToken().oauthToken(" ")));
    assertThatThrownBy(this::readCredentials).isExactlyInstanceOf(CatalogException.class);
  }

  private void vend(TemporaryCredentials minted) {
    when(vendor.vendCredential(
            any(NormalizedURL.class), eq(CredentialContext.READ_ONLY), eq(CredentialUse.CLIENT)))
        .thenReturn(minted);
  }

  private StorageCredentials readCredentials() {
    CredentialRequest read =
        new CredentialRequest(
            AssetType.TABLE, ORDERS.fullName(), null, null, StorageOperation.READ, null);
    return connector.getStorageCredentials(read, auth()).get(0);
  }

  /** Orders is a Delta table in a catalog and schema the user may use. */
  private void ordersTable(boolean maySelect) {
    ordersTable(maySelect, "s3://bucket/orders");
  }

  private void ordersTable(boolean maySelect, String storageLocation) {
    when(users.getUser(USER_ID))
        .thenReturn(
            new User().id(USER_ID).email("alice@example.com").state(User.StateEnum.ENABLED));
    when(tables.getTable(ORDERS.fullName()))
        .thenReturn(
            new TableInfo()
                .tableId(TABLE_ID.toString())
                .catalogName("main")
                .schemaName("sales")
                .name("orders")
                .dataSourceFormat(DataSourceFormat.DELTA)
                .storageLocation(storageLocation));
    when(keyMapper.mapResourceKeys(Map.of(TABLE, ORDERS.fullName())))
        .thenReturn(Map.of(CATALOG, CATALOG_ID, SCHEMA, SCHEMA_ID, TABLE, TABLE_ID));
    when(metastore.getMetastoreId()).thenReturn(UUID.randomUUID());
    when(authorizer.authorizeAny(any(), eq(CATALOG), eq(CATALOG_ID), any(Privileges[].class)))
        .thenReturn(true);
    when(authorizer.authorizeAny(any(), eq(SCHEMA), eq(SCHEMA_ID), any(Privileges[].class)))
        .thenReturn(true);
    when(authorizer.authorizeAny(any(), eq(TABLE), eq(TABLE_ID), any(Privileges[].class)))
        .thenReturn(maySelect);
  }

  private static AuthContext auth() {
    return AuthContext.of(UserContext.fromUserIdAndName(USER_ID, null));
  }
}
