package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SqlNameValidationTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "test",
        "sample_volume_name",
        "with-hyphen@at",
        "issue$components",
        "issue#1",
        "a+b",
        "Grüße_äöü"
      })
  public void testValidName(String name) {
    assertThatCode(() -> ValidationUtils.validateSqlObjectName(name)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"test.", "test ", "test/test", "a\tb", "a\nb", "a\u0000b", "a\u007Fb"})
  public void testNameWithInvalidCharacter(String name) {
    assertInvalid(
        name, "Name cannot contain a period, space, forward-slash, or control characters");
  }

  @Test
  public void testNameLength() {
    assertThatCode(() -> ValidationUtils.validateSqlObjectName("a".repeat(255)))
        .doesNotThrowAnyException();
    assertInvalid("a".repeat(256), "Name cannot be longer than 255 characters");
    assertInvalid("", "Name cannot be empty");
  }

  private static void assertInvalid(String name, String message) {
    assertThatThrownBy(() -> ValidationUtils.validateSqlObjectName(name))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(message)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }
}
