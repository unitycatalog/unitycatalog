package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.model.SecurableType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class AuthorizeKeyLocatorTest {

  @Test
  public void resourceKeyVariableNameUsesSecurableType() {
    AuthorizeKeyLocator l =
        AuthorizeKeyLocator.builder()
            .source(AuthorizeKeyLocator.Source.PAYLOAD)
            .type(Optional.of(SecurableType.EXTERNAL_LOCATION))
            .key("location")
            .build();
    // For resource keys, the SpEL variable is always the securable's type name regardless of the
    // payload lookup key. This lets DRC's kebab-case payload field "location" surface as the same
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
    // Kebab-case keys (DRC payload shape) must surface as valid SpEL identifiers — hyphens in the
    // last path segment map to underscores. The payload lookup itself still uses the original key.
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
    // Existing snake_case keys keep their behavior — the hyphen→underscore transform is a no-op.
    assertThat(l.getVariableName()).isEqualTo("table_type");
  }
}
