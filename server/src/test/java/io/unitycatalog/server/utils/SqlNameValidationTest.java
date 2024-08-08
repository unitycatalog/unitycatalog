package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import org.junit.jupiter.api.Test;

public class SqlNameValidationTest {

  @Test
  public void testValidateSqlObjectName() {
    ValidationUtils.validateSqlObjectName("test");
    ValidationUtils.validateSqlObjectName("sample_volume_name");
    assertThatThrownBy(() -> ValidationUtils.validateSqlObjectName("test."))
        .isInstanceOf(BaseException.class);
    assertThatThrownBy(() -> ValidationUtils.validateSqlObjectName("test "))
        .isInstanceOf(BaseException.class);
    assertThatThrownBy(() -> ValidationUtils.validateSqlObjectName("test/test"))
        .isInstanceOf(BaseException.class);
    char ctrlC = 3; // ASCII value for Control-C
    assertThatThrownBy(() -> ValidationUtils.validateSqlObjectName("test" + ctrlC))
        .isInstanceOf(BaseException.class);
  }
}
