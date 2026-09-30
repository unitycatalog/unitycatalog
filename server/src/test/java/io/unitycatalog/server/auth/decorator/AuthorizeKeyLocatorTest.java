package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.model.SecurableType;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class AuthorizeKeyLocatorTest {

  /** A stateless extractor with a public no-arg constructor, as the mechanism requires. */
  public static class UpperCaseExtractor implements AuthorizeValueExtractor {
    @Override
    public Object extract(Object body) {
      return ((String) body).toUpperCase();
    }
  }

  @Test
  public void resourceKeyVariableNameUsesSecurableType() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.of(SecurableType.EXTERNAL_LOCATION))
            .key("location")
            .build();
    // For resource keys, the SpEL variable is always the securable's type name regardless of the
    // payload lookup key. This lets Delta's kebab-case payload field "location" surface as the same
    // #external_location variable that the snake_case "storage_location" key uses.
    assertThat(l.getVariableName()).isEqualTo("external_location");
  }

  @Test
  public void plainKeyVariableNameStripsPathPrefix() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("config.operation")
            .build();
    assertThat(l.getVariableName()).isEqualTo("operation");
  }

  @Test
  public void plainKeyVariableNameMapsHyphensToUnderscores() {
    // Kebab-case keys (Delta payload shape) must surface as valid SpEL identifiers: hyphens in
    // the last path segment map to underscores. The payload lookup itself still uses the original
    // key.
    AuthorizeKeyLocator hyphen =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("table-type")
            .build();
    assertThat(hyphen.getVariableName()).isEqualTo("table_type");
    assertThat(hyphen.getKey()).isEqualTo("table-type");

    // Hyphens in nested path segments survive the prefix-strip + transform combo.
    AuthorizeKeyLocator nested =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("outer.inner-field")
            .build();
    assertThat(nested.getVariableName()).isEqualTo("inner_field");
  }

  @Test
  public void plainSnakeCaseKeyIsUnchanged() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("table_type")
            .build();
    // Existing snake_case keys keep their behavior: the hyphen-to-underscore transform is a no-op.
    assertThat(l.getVariableName()).isEqualTo("table_type");
  }

  @Test
  public void findPayloadValueLooksUpKeyWhenNoExtractor() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("config.operation")
            .build();
    // Nested lookup resolves against the pre-parsed body map; a missing key resolves to null.
    assertThat(l.findPayloadValue(null, Map.of("config", Map.of("operation", "READ"))))
        .isEqualTo("READ");
    assertThat(l.findPayloadValue(null, Map.of("config", Map.of()))).isNull();
  }

  @Test
  public void findPayloadValueUsesExtractorAndIgnoresKey() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.empty())
            .key("ignored")
            .extractor(new UpperCaseExtractor())
            .build();
    // The extractor computes the value from the body; the key is not consulted.
    assertThat(l.findPayloadValue("abc", null)).isEqualTo("ABC");
  }

  @Test
  public void fromResourceKeyWithExtractorIsAPayloadLocator() {
    Parameter parameter = parameterOf("resourceWithExtractor", String.class);
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.from(parameter.getAnnotation(AuthorizeResourceKey.class), parameter);
    assertThat(l.getSource()).isEqualTo(AuthorizeKeyLocator.Source.PAYLOAD);
    assertThat(l.getType()).contains(SecurableType.EXTERNAL_LOCATION);
    assertThat(l.findPayloadValue("abc", null)).isEqualTo("ABC");
  }

  @Test
  public void fromRejectsExtractorCombinedWithParam() {
    Parameter parameter = parameterOf("extractorWithParam", String.class);
    AuthorizeKey key = parameter.getAnnotation(AuthorizeKey.class);
    assertThatThrownBy(() -> AuthorizeKeyLocator.from(key, parameter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("@Param");
  }

  @Test
  public void fromRejectsAuthorizeKeyExtractorWithoutKey() {
    Parameter parameter = parameterOf("extractorWithoutKey", String.class);
    AuthorizeKey key = parameter.getAnnotation(AuthorizeKey.class);
    assertThatThrownBy(() -> AuthorizeKeyLocator.from(key, parameter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("key");
  }

  @Test
  public void fromResourceKeyCarriesSkipWhen() {
    Parameter parameter = parameterOf("resourceWithSkipWhen", String.class);
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.from(parameter.getAnnotation(AuthorizeResourceKey.class), parameter);
    assertThat(l.getSource()).isEqualTo(AuthorizeKeyLocator.Source.PARAM);
    assertThat(l.getSkipWhen()).isEqualTo("staged_create");
  }

  @SneakyThrows
  private static Parameter parameterOf(String methodName, Class<?>... paramTypes) {
    return AuthorizeKeyLocatorTest.class.getDeclaredMethod(methodName, paramTypes)
        .getParameters()[0];
  }

  // --- sample methods whose parameter annotations drive the from(...) tests -------------------

  @SuppressWarnings("unused")
  private void resourceWithExtractor(
      @AuthorizeResourceKey(
              value = SecurableType.EXTERNAL_LOCATION,
              extractor = UpperCaseExtractor.class)
          String body) {}

  @SuppressWarnings("unused")
  private void resourceWithSkipWhen(
      @Param("table") @AuthorizeResourceKey(value = SecurableType.TABLE, skipWhen = "staged_create")
          String table) {}

  @SuppressWarnings("unused")
  private void extractorWithParam(
      @Param("x") @AuthorizeKey(key = "x", extractor = UpperCaseExtractor.class) String body) {}

  @SuppressWarnings("unused")
  private void extractorWithoutKey(
      @AuthorizeKey(extractor = UpperCaseExtractor.class) String body) {}
}
