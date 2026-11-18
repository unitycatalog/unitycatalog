package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.unitycatalog.server.exception.ErrorCode;
import java.sql.SQLException;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DatabaseErrorsTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "uk_uc_tables_schema_name",
        "uc_tables.uk_uc_tables_schema_name",
        "PUBLIC.UK_UC_TABLES_SCHEMA_NAME_INDEX_9"
      })
  void recognizesOnlyTheTableNameKey(String constraint) {
    var failure =
        new ConstraintViolationException("conflict", new SQLException("duplicate"), constraint);
    assertThat(DatabaseErrors.errorCode(mock(SessionFactory.class), failure))
        .contains(ErrorCode.TABLE_ALREADY_EXISTS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"uk_other", "uk_uc_tables_schema_name_other", "id", ""})
  void doesNotRelabelUnrelatedConstraintFailures(String constraint) {
    var failure =
        new ConstraintViolationException("conflict", new SQLException("duplicate"), constraint);
    assertThat(DatabaseErrors.errorCode(mock(SessionFactory.class), failure)).isEmpty();
  }
}
