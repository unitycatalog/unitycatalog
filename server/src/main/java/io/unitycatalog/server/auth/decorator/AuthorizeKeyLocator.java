package io.unitycatalog.server.auth.decorator;

import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.model.SecurableType;
import java.lang.reflect.Parameter;
import java.util.Map;
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
public class AuthorizeKeyLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizeKeyLocator.class);

  public enum Source {
    SYSTEM,
    PARAM,
    PAYLOAD;
  }

  // For AuthorizeKey, type is empty; For AuthorizeResourceKey, type is the resource securable type.
  private Optional<SecurableType> type;
  private Source source;
  private String key;

  // Custom value extractor, or null to look the value up by key. Only set on PAYLOAD locators.
  private AuthorizeValueExtractor extractor;

  // For a resource key, the name of a non-resource variable that, when truthy, skips resolving this
  // key. Null/empty means always resolve. See AuthorizeResourceKey#skipWhen.
  private String skipWhen;

  /**
   * Extracts the variable name from a key. For resources, returns their securable type as variable
   * name. e.g. "external_location" For other keys, returns the key name in annotated parameters or
   * the last segment of the payload parameter. e.g. "config.operation" returns "operation".
   *
   * <p>Hyphens in the key are mapped to underscores so kebab-case payload fields (such as Delta
   * REST Catalog's {@code table-type}) form valid SpEL identifiers like {@code #table_type}. The
   * payload lookup still uses the original key verbatim; this transformation affects only the SpEL
   * variable name.
   */
  public String getVariableName() {
    if (type.isPresent()) {
      return type.get().getValue();
    }
    int lastDot = key.lastIndexOf('.');
    String name = lastDot >= 0 ? key.substring(lastDot + 1) : key;
    return name.replace('-', '_');
  }

  /**
   * Finds this locator's value from the request body. The caller supplies the body in the two forms
   * the locator may need: {@code body} is the typed object the handler bound, and {@code bodyMap}
   * is that same object as a JSON map. An {@link #extractor} reads {@code body}; a {@link #key}
   * lookup walks {@code bodyMap}. Only valid for PAYLOAD locators.
   *
   * @param body the request body as its bound model object (the argument the handler receives, e.g.
   *     an {@code UpdateTableRequest}); read by the {@link #extractor} when one is set.
   * @param bodyMap the same body re-serialized once to a nested {@code Map<String, Object>} (its
   *     JSON tree, keyed by wire names via the mapper's naming strategy / {@code @JsonProperty});
   *     walked by the {@link #key} lookup. May be null when this locator uses an extractor.
   * @return the located value, or null when the key is absent from {@code bodyMap}.
   */
  public Object findPayloadValue(Object body, Map<String, Object> bodyMap) {
    if (extractor != null) {
      return extractor.extract(body);
    }
    return findNestedValue(key, bodyMap);
  }

  private static Object findNestedValue(String key, Map<String, Object> map) {
    // TODO: investigate better object traversal functionality
    String[] args = key.split("[.]", 2);
    if (args.length == 1) {
      return map.get(args[0]);
    }
    if (map.get(args[0]) instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> value = (Map<String, Object>) map.get(args[0]);
      return findNestedValue(args[1], value);
    }
    return null;
  }

  public static AuthorizeKeyLocator from(AuthorizeResourceKey key, Parameter parameter) {
    return from(
        Optional.of(key.value()),
        key.key(),
        extractorOf(key.extractor()),
        key.skipWhen(),
        parameter);
  }

  public static AuthorizeKeyLocator from(AuthorizeKey key, Parameter parameter) {
    return from(Optional.empty(), key.key(), extractorOf(key.extractor()), "", parameter);
  }

  private static AuthorizeKeyLocator from(
      Optional<SecurableType> type,
      String key,
      AuthorizeValueExtractor extractor,
      String skipWhen,
      Parameter parameter) {
    Param param = parameter.getAnnotation(Param.class);
    if (extractor != null) {
      // A custom extractor reads the value from the bound body, so the source is PAYLOAD (deferred
      // to the gate converter) and no @Param may be present. A resource key takes its variable name
      // from its securable type; a raw AuthorizeKey still needs a key to name its SpEL variable.
      if (param != null) {
        throw new IllegalStateException(
            "Authorization extractor on parameter "
                + parameter.getName()
                + " cannot be combined with @Param");
      }
      if (type.isEmpty() && key.isEmpty()) {
        throw new IllegalStateException(
            "@AuthorizeKey with an extractor must set key to name its SpEL variable: "
                + parameter.getName());
      }
      return AuthorizeKeyLocator.builder()
          .source(Source.PAYLOAD)
          .type(type)
          .key(key)
          .extractor(extractor)
          .skipWhen(skipWhen)
          .build();
    }
    if (param != null) {
      // @Param bound: source is the URL query/path. If a key is explicitly set it must equal
      // @Param.value() so the SpEL variable name and URL parameter name agree; leaving it empty
      // reuses @Param.value() for both.
      if (!key.isEmpty() && !key.equals(param.value())) {
        throw new IllegalStateException(
            "Authorization key=\""
                + key
                + "\" on parameter "
                + parameter.getName()
                + " must match its companion @Param(\""
                + param.value()
                + "\")");
      }
      return AuthorizeKeyLocator.builder()
          .source(Source.PARAM)
          .type(type)
          .key(param.value())
          .skipWhen(skipWhen)
          .build();
    }
    // No @Param: the key names a request body field.
    if (key.isEmpty()) {
      throw new RuntimeException(
          "Couldn't find param key for authorization key: " + parameter.getName());
    }
    return AuthorizeKeyLocator.builder()
        .source(Source.PAYLOAD)
        .type(type)
        .key(key)
        .skipWhen(skipWhen)
        .build();
  }

  /**
   * Instantiates the annotation's extractor class, or returns null for the sentinel default ({@link
   * AuthorizeValueExtractor} itself) that means "no custom extractor".
   */
  private static AuthorizeValueExtractor extractorOf(
      Class<? extends AuthorizeValueExtractor> extractorClass) {
    if (extractorClass == AuthorizeValueExtractor.class) {
      return null;
    }
    try {
      return extractorClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot instantiate authorization value extractor " + extractorClass.getName(), e);
    }
  }
}
