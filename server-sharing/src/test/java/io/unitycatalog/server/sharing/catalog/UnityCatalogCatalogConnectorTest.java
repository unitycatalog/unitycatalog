package io.unitycatalog.server.sharing.catalog;

import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opensharing.catalog.AssetLookup;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.CatalogCaller;
import io.opensharing.catalog.CloudProvider;
import io.opensharing.catalog.CredentialRequest;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.catalog.StorageCredentialKeys;
import io.opensharing.catalog.StorageCredentials;
import io.opensharing.catalog.StorageOperation;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.UnityCatalogIdentityService;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnityCatalogCatalogConnectorTest {

  private static final UUID PRINCIPAL_ID = UUID.randomUUID();
  private static final UUID METASTORE_ID = UUID.randomUUID();
  private static final UUID CATALOG_ID = UUID.randomUUID();
  private static final UUID SCHEMA_ID = UUID.randomUUID();
  private static final UUID TABLE_ID = UUID.randomUUID();

  private final Repositories repositories = mock(Repositories.class);
  private final TableRepository tables = mock(TableRepository.class);
  private final SchemaRepository schemas = mock(SchemaRepository.class);
  private final CatalogRepository catalogs = mock(CatalogRepository.class);
  private final MetastoreRepository metastore = mock(MetastoreRepository.class);
  private final StorageCredentialVendor credentialVendor = mock(StorageCredentialVendor.class);
  private final UnityCatalogIdentityService identities =
      mock(UnityCatalogIdentityService.class);

  @BeforeEach
  void setUp() {
    when(repositories.getTableRepository()).thenReturn(tables);
    when(repositories.getSchemaRepository()).thenReturn(schemas);
    when(repositories.getCatalogRepository()).thenReturn(catalogs);
    when(repositories.getMetastoreRepository()).thenReturn(metastore);
    when(repositories.getStorageCredentialVendor()).thenReturn(credentialVendor);
    when(metastore.getMetastoreId()).thenReturn(METASTORE_ID);
    when(identities.authenticate("token"))
        .thenReturn(new UnityCatalogIdentityService.Identity(PRINCIPAL_ID, "alice", null));
    when(identities.onBehalfOf("owner-id", "owner"))
        .thenReturn(new UnityCatalogIdentityService.Identity(PRINCIPAL_ID, "owner", null));
    when(schemas.getSchema("main.sales"))
        .thenReturn(
            new SchemaInfo()
                .catalogName("main")
                .name("sales")
                .fullName("main.sales")
                .schemaId(SCHEMA_ID.toString()));
    when(catalogs.getCatalog("main"))
        .thenReturn(new CatalogInfo().name("main").id(CATALOG_ID.toString()));
  }

  @Test
  void resolvesTablesWithTheSameRepositoryAndAuthorizationModelAsUc() {
    TableInfo table = table("orders", TABLE_ID, "s3://bucket/orders");
    when(tables.getTable("main.sales.orders")).thenReturn(table);
    UnityCatalogCatalogConnector connector =
        connector(new AllowingAuthorizer());

    ResolvedAsset result =
        connector.resolveAsset(
            AssetLookup.of(AssetType.TABLE, "main.sales.orders"),
            CatalogCaller.withBearerToken("alice", "token"));

    assertEquals(TABLE_ID.toString(), result.catalogAssetId());
    assertEquals("s3://bucket/orders", result.storageLocation());
    assertTrue(result.accessModes().stream().anyMatch(mode -> mode.name().equals("DIR")));
  }

  @Test
  void trustedOnBehalfOfCallsUseTheStoredUcUserIdDirectly() {
    when(tables.getTable("main.sales.orders"))
        .thenReturn(table("orders", TABLE_ID, "s3://bucket/orders"));
    UnityCatalogCatalogConnector connector =
        connector(new AllowingAuthorizer());

    connector.resolveAsset(
        AssetLookup.of(AssetType.TABLE, "main.sales.orders"),
        CatalogCaller.onBehalfOf("owner", "owner-id"));

    verify(identities).onBehalfOf("owner-id", "owner");
  }

  @Test
  void mapsUcStorageCredentialsWithoutHttpSerialization() {
    TableInfo table = table("orders", TABLE_ID, "s3://bucket/orders");
    when(tables.getTable("main.sales.orders")).thenReturn(table);
    when(credentialVendor.vendCredential(
            eq(NormalizedURL.from("s3://bucket/orders")), eq(CredentialContext.READ_ONLY)))
        .thenReturn(
            new TemporaryCredentials()
                .url("s3://bucket/orders")
                .expirationTime(42L)
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("access")
                        .secretAccessKey("secret")
                        .sessionToken("session")));
    UnityCatalogCatalogConnector connector =
        connector(new AllowingAuthorizer());

    List<StorageCredentials> result =
        connector.getStorageCredentials(
            new CredentialRequest(
                AssetType.TABLE,
                "main.sales.orders",
                TABLE_ID.toString(),
                "s3://bucket/orders",
                StorageOperation.READ,
                Duration.ofMinutes(5)),
            CatalogCaller.withBearerToken("alice", "token"));

    assertEquals(CloudProvider.AWS, result.getFirst().provider());
    assertEquals(
        "access",
        result.getFirst().credentials().get(StorageCredentialKeys.ACCESS_KEY_ID));
    assertEquals(42L, result.getFirst().expiration().toEpochMilli());
  }

  @Test
  void schemaListingsFilterTablesTheCallerCannotSelect() {
    UUID deniedId = UUID.randomUUID();
    TableInfo allowed = table("allowed", TABLE_ID, "s3://bucket/allowed");
    TableInfo denied = table("denied", deniedId, "s3://bucket/denied");
    when(tables.getTable("main.sales.allowed")).thenReturn(allowed);
    when(tables.getTable("main.sales.denied")).thenReturn(denied);
    when(tables.listTables(
            eq("main"), eq("sales"), any(), eq(Optional.empty()), eq(false), eq(false)))
        .thenReturn(new ListTablesResponse().tables(List.of(allowed, denied)));
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.refreshAuthorizations()).thenReturn(false);
    when(authorizer.authorize(eq(PRINCIPAL_ID), any(), any()))
        .thenAnswer(
            invocation -> {
              Privileges privilege = invocation.getArgument(2);
              if (privilege == OWNER) {
                return false;
              }
              return privilege != SELECT || TABLE_ID.equals(invocation.getArgument(1));
            });
    when(authorizer.authorizeAny(eq(PRINCIPAL_ID), any(), any(Privileges[].class)))
        .thenAnswer(
            invocation -> {
              return Arrays.asList(invocation.getArguments()).contains(SELECT)
                  ? TABLE_ID.equals(invocation.getArgument(1))
                  : true;
            });
    UnityCatalogCatalogConnector connector = connector(authorizer);

    List<ResolvedAsset> result =
        connector.listChildren(
            AssetLookup.of(AssetType.SCHEMA, "main.sales"),
            CatalogCaller.withBearerToken("alice", "token"));

    assertEquals(
        List.of("main.sales.allowed"),
        result.stream().map(ResolvedAsset::identifier).toList());
  }

  private UnityCatalogCatalogConnector connector(UnityCatalogAuthorizer authorizer) {
    return new UnityCatalogCatalogConnector(
        repositories, authorizer, identities, credentialVendor);
  }

  private static TableInfo table(String name, UUID id, String location) {
    return new TableInfo()
        .catalogName("main")
        .schemaName("sales")
        .name(name)
        .tableId(id.toString())
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation(location);
  }
}
