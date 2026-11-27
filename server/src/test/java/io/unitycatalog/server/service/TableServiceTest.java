package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.utils.ServerProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Verifies that existence checks preserve failures other than missing catalog objects. */
class TableServiceTest {
  @ParameterizedTest
  @EnumSource(
      value = ErrorCode.class,
      names = {"INTERNAL", "PERMISSION_DENIED", "INVALID_ARGUMENT", "NOT_FOUND", "UNAVAILABLE"})
  void tableExistsPreservesLookupErrors(ErrorCode errorCode) {
    Repositories repositories = mock(Repositories.class);
    TableRepository tables = mock(TableRepository.class);
    when(repositories.getTableRepository()).thenReturn(tables);
    BaseException failure = new BaseException(errorCode, "Lookup failed");
    when(tables.findTableOrThrow("catalog", "schema", "table")).thenThrow(failure);
    TableService service =
        new TableService(
            mock(UnityCatalogAuthorizer.class), repositories, mock(ServerProperties.class));

    assertThatThrownBy(() -> service.tableExists("catalog.schema.table")).isSameAs(failure);
  }
}
