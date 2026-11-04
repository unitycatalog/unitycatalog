package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.BROWSE;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.USE_CATALOG;
import static io.unitycatalog.server.persist.model.Privileges.USE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ResultFilterTest {

  @Test
  void listFilteringKeepsEachItemsOwnDecision() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID metastore = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    when(authorizer.authorize(principal, CATALOG, catalog, USE_CATALOG)).thenReturn(true);
    when(authorizer.authorize(principal, SCHEMA, schema, USE_SCHEMA)).thenReturn(true);
    List<TableInfo> items = new ArrayList<>();
    List<String> readable = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      UUID table = UUID.randomUUID();
      items.add(new TableInfo().tableId(table.toString()));
      if (i % 2 == 0) {
        readable.add(table.toString());
      }
    }
    when(authorizer.authorizeAny(
            eq(principal), eq(TABLE), any(UUID.class), any(Privileges[].class)))
        .thenAnswer(invocation -> readable.contains(invocation.getArgument(2).toString()));

    new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            principal,
            AuthorizeExpressions.GET_TABLE,
            "",
            new HashMap<>(Map.of(METASTORE, metastore, CATALOG, catalog, SCHEMA, schema)),
            Map.of(),
            mock(KeyMapper.class))
        .filter(TABLE, items);

    // Decisions are shared across items only for the same resource: alternating table grants must
    // still alternate after filtering.
    assertThat(items).extracting(TableInfo::getTableId).containsExactlyElementsOf(readable);
    verify(authorizer, times(1)).authorize(principal, CATALOG, catalog, USE_CATALOG);
    verify(authorizer, times(1)).authorize(principal, SCHEMA, schema, USE_SCHEMA);
  }

  @Test
  void mixedListKeepsFullMetadataFirstAndFiltersDeniedOrInvalidItems() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID fullId = UUID.randomUUID();
    UUID browseId = UUID.randomUUID();
    CatalogInfo full = new CatalogInfo().name("full").id(fullId.toString());
    CatalogInfo browse = new CatalogInfo().name("browse").id(browseId.toString());
    when(authorizer.authorizeAny(principal, CATALOG, fullId, OWNER, USE_CATALOG)).thenReturn(true);
    when(authorizer.authorize(principal, CATALOG, fullId, BROWSE)).thenReturn(true);
    when(authorizer.authorize(principal, CATALOG, browseId, BROWSE)).thenReturn(true);
    List<CatalogInfo> items =
        new ArrayList<>(
            List.of(
                full,
                browse,
                new CatalogInfo().name("hidden").id(UUID.randomUUID().toString()),
                new CatalogInfo().name("invalid").id("invalid-id")));
    ResultFilter filter =
        new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            principal,
            AuthorizeExpressions.GET_CATALOG,
            AuthorizeExpressions.BROWSE_CATALOG,
            Map.of(METASTORE, UUID.randomUUID()),
            Map.of(),
            mock(KeyMapper.class));

    filter.filter(CATALOG, items, BrowseOnlyConverter::toBrowseOnly);

    assertThat(items).containsExactly(full, BrowseOnlyConverter.toBrowseOnly(browse));
    assertThat(items.get(0)).isSameAs(full);
    assertThat(browse.getBrowseOnly()).isNull();
    assertThat(filter.wasCalled()).isTrue();
    verify(authorizer, never()).authorize(principal, CATALOG, fullId, BROWSE);
  }

  @ParameterizedTest
  @CsvSource({"false,true,true", "true,false,true", "true,true,false"})
  void fallbackRequiresOptInGrantAndProjection(
      boolean includeBrowse, boolean browseGranted, boolean hasProjection) throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalogId = UUID.randomUUID();
    when(authorizer.authorize(principal, CATALOG, catalogId, BROWSE)).thenReturn(browseGranted);
    ResultFilter filter =
        new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            principal,
            AuthorizeExpressions.GET_CATALOG,
            includeBrowse ? AuthorizeExpressions.BROWSE_CATALOG : "",
            Map.of(CATALOG, catalogId, METASTORE, UUID.randomUUID()),
            Map.of(),
            mock(KeyMapper.class));
    UnaryOperator<CatalogInfo> projection =
        hasProjection ? BrowseOnlyConverter::toBrowseOnly : null;

    assertThatThrownBy(
            () ->
                filter.filterResult(
                    CATALOG, new CatalogInfo().id(catalogId.toString()), projection))
        .isInstanceOfSatisfying(
            BaseException.class,
            exception ->
                assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED));
    assertThat(filter.wasCalled()).isTrue();
  }

  @ParameterizedTest
  @CsvSource({"true,true", "false,true", "true,false", "false,false"})
  void getRequiresMatchingResourceId(boolean fullAccess, boolean sameId) throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalogId = UUID.randomUUID();
    CatalogInfo item =
        new CatalogInfo().name("catalog").id((sameId ? catalogId : UUID.randomUUID()).toString());
    when(authorizer.authorizeAny(principal, CATALOG, catalogId, OWNER, USE_CATALOG, READ_METADATA))
        .thenReturn(fullAccess);
    when(authorizer.authorize(principal, CATALOG, catalogId, BROWSE)).thenReturn(true);
    ResultFilter filter =
        new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            principal,
            AuthorizeExpressions.GET_CATALOG,
            fullAccess ? "" : AuthorizeExpressions.BROWSE_CATALOG,
            Map.of(CATALOG, catalogId, METASTORE, UUID.randomUUID()),
            Map.of(),
            mock(KeyMapper.class));

    if (sameId) {
      assertThat(filter.filterResult(CATALOG, item, BrowseOnlyConverter::toBrowseOnly))
          .isEqualTo(fullAccess ? item : BrowseOnlyConverter.toBrowseOnly(item));
    } else {
      assertThatThrownBy(
              () -> filter.filterResult(CATALOG, item, BrowseOnlyConverter::toBrowseOnly))
          .isInstanceOfSatisfying(
              BaseException.class,
              exception ->
                  assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED));
    }
  }
}
