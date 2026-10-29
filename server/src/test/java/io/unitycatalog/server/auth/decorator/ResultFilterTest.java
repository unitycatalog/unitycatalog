package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.persist.model.Privileges.BROWSE;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.USE_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ResultFilterTest {
  @Test
  void mixedListKeepsFullMetadataFirstAndFiltersDeniedOrInvalidItems() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID fullId = UUID.randomUUID();
    UUID browseId = UUID.randomUUID();
    CatalogInfo full = new CatalogInfo().name("full").id(fullId.toString());
    CatalogInfo browse = new CatalogInfo().name("browse").id(browseId.toString());
    when(authorizer.authorizeAny(principal, fullId, OWNER, USE_CATALOG)).thenReturn(true);
    when(authorizer.authorize(principal, fullId, BROWSE)).thenReturn(true);
    when(authorizer.authorize(principal, browseId, BROWSE)).thenReturn(true);
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
    verify(authorizer, never()).authorize(principal, fullId, BROWSE);
  }

  @ParameterizedTest
  @CsvSource({"false,true,true", "true,false,true", "true,true,false"})
  void fallbackRequiresOptInGrantAndProjection(
      boolean includeBrowse, boolean browseGranted, boolean hasProjection) throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalogId = UUID.randomUUID();
    when(authorizer.authorize(principal, catalogId, BROWSE)).thenReturn(browseGranted);
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
            () -> filter.filterResult(new CatalogInfo().id(catalogId.toString()), projection))
        .isInstanceOfSatisfying(
            BaseException.class,
            exception ->
                assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED));
    assertThat(filter.wasCalled()).isTrue();
  }
}
