package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @Test
  public void splitFullNameReturnsTheParts() {
    assertThat(ValidationUtils.splitFullName("a.b.c", 3, "Invalid name"))
        .containsExactly("a", "b", "c");
    assertThat(ValidationUtils.splitFullName("a.b", 2, "Invalid name")).containsExactly("a", "b");
  }

  @ParameterizedTest
  @CsvSource({".b.c, 3", "a..c, 3", "a.b., 3", "a., 2"})
  public void splitFullNameRejectsAnEmptyPart(String name, int parts) {
    assertRejected(name, parts);
  }

  @ParameterizedTest
  @CsvSource({"a.b, 3", "a.b.c.d, 3", "a.b.c., 3", "'', 3", "a.b.c, 2"})
  public void splitFullNameRejectsTheWrongNumberOfParts(String name, int parts) {
    assertRejected(name, parts);
  }

  private static void assertRejected(String name, int parts) {
    assertThatThrownBy(() -> ValidationUtils.splitFullName(name, parts, "Invalid name: " + name))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessage("Invalid name: " + name);
  }
}
