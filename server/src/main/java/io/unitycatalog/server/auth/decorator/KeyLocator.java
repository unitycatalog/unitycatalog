package io.unitycatalog.server.auth.decorator;

import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.model.SecurableType;
import java.lang.reflect.Parameter;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locator for authorization parameters extracted from request URL parameter or payloads.
 *
 * <p>It finds out values from both {@link AuthorizeKey} and {@link AuthorizeResourceKey}
 * annotations.
 *
 * <p>Example: For {@code @AuthorizeKey(key = "operation")}, this locator would extract the
 * "operation" field value from the request payload and make it available as "#operation" in the
 * SpEL authorization expression.
 *
 * @see AuthorizeKey
 * @see AuthorizeResourceKey
 */
@Builder
@Getter
public class KeyLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyLocator.class);

  public enum Source {
    SYSTEM,
    PARAM,
    PAYLOAD;
  }

  // For AuthorizeKey, type is empty; For AuthorizeResourceKey, type is the resource securable type.
  private Optional<SecurableType> type;
  private Source source;
  private String key;

  /**
   * Extracts the variable name from a key. For resources, returns their securable type as variable
   * name. e.g. "external_location" For other keys, returns the key name in annotated parameters or
   * the last segment of the payload parameter. e.g. "config.operation" returns "operation"
   */
  public String getVariableName() {
    if (type.isPresent()) {
      return type.get().getValue();
    }
    int lastDot = key.lastIndexOf('.');
    return lastDot >= 0 ? key.substring(lastDot + 1) : key;
  }

  public static KeyLocator from(AuthorizeResourceKey key, Parameter parameter) {
    return from(Optional.of(key.value()), key.key(), parameter);
  }

  public static KeyLocator from(AuthorizeKey key, Parameter parameter) {
    return from(Optional.empty(), key.key(), parameter);
  }

  private static KeyLocator from(Optional<SecurableType> type, String key, Parameter parameter) {
    if (!key.isEmpty()) {
      // Explicitly declaring a key, so it's the source is from the payload data
      return KeyLocator.builder().source(Source.PAYLOAD).type(type).key(key).build();
    } else {
      // No key defined so implicitly referencing an (annotated) (query) parameter
      Param param = parameter.getAnnotation(Param.class);
      if (param != null) {
        return KeyLocator.builder().source(Source.PARAM).type(type).key(param.value()).build();
      } else {
        throw new RuntimeException(
            "Couldn't find param key for authorization key: " + parameter.getName());
      }
    }
  }
}
