package io.unitycatalog.server.sharing.catalog;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.opensharing.auth.AuthContext;
import io.opensharing.auth.Privilege;
import io.opensharing.auth.UserContext;
import io.opensharing.catalog.Asset;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.exception.AssetAccessDeniedException;
import io.opensharing.exception.CatalogAuthorizationException;
import io.opensharing.exception.CatalogException;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.ServerProperties;
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

  private final UserRepository users = mock(UserRepository.class);
  private final TableRepository tables = mock(TableRepository.class);
  private final MetastoreRepository metastore = mock(MetastoreRepository.class);
  private final KeyMapper keyMapper = mock(KeyMapper.class);
  private final UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
  private final UnityCatalogCatalogConnector connector;

  public UnityCatalogCatalogConnectorTest() {
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(users);
    when(repositories.getTableRepository()).thenReturn(tables);
    when(repositories.getMetastoreRepository()).thenReturn(metastore);
    when(repositories.getKeyMapper()).thenReturn(keyMapper);
    ServerProperties properties = mock(ServerProperties.class);
    when(properties.getS3Configurations()).thenReturn(Map.of());
    connector = new UnityCatalogCatalogConnector(repositories, authorizer, properties);
  }

  @Test
  public void grantsSharingPrivilegesToAnEnabledUserWithoutAskingTheAuthorizer() {
    when(users.getUser(USER_ID))
        .thenReturn(
            new User().id(USER_ID).email("alice@example.com").state(User.StateEnum.ENABLED));
    for (Privilege privilege : Privilege.values()) {
      assertThat(connector.authorize(auth(), privilege).userId()).isEqualTo(USER_ID);
    }
    verifyNoInteractions(authorizer);
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

  /** Orders is a Delta table in a catalog and schema the user may use. */
  private void ordersTable(boolean maySelect) {
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
                .storageLocation("s3://bucket/orders"));
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
