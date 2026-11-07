package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.USE_CATALOG;
import static io.unitycatalog.server.persist.model.Privileges.USE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

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
  void listFilteringPropagatesParentResolutionFailures() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    KeyMapper keyMapper = mock(KeyMapper.class);
    RegisteredModelInfo model =
        new RegisteredModelInfo()
            .id(UUID.randomUUID().toString())
            .catalogName("catalog")
            .schemaName("schema");
    BaseException failure = new BaseException(ErrorCode.INTERNAL, "Failed to load model parents");
    RuntimeException unexpectedFailure =
        new IllegalStateException("Failed to open database session");
    when(keyMapper.mapResourceKeys(
            Map.of(CATALOG, model.getCatalogName(), SCHEMA, model.getSchemaName())))
        .thenThrow(failure, unexpectedFailure);
    ResultFilter filter =
        new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            UUID.randomUUID(),
            "#authorize(#principal, #registered_model, OWNER)",
            Map.of(),
            Map.of(),
            keyMapper);

    assertThatThrownBy(() -> filter.filter(REGISTERED_MODEL, new ArrayList<>(List.of(model))))
        .isSameAs(failure);

    assertThatThrownBy(() -> filter.filter(REGISTERED_MODEL, new ArrayList<>(List.of(model))))
        .isInstanceOf(BaseException.class)
        .hasMessage("Failed to authorize list items")
        .hasCause(unexpectedFailure)
        .extracting(error -> ((BaseException) error).getErrorCode())
        .isEqualTo(ErrorCode.INTERNAL);
    verifyNoInteractions(authorizer);
  }
}
