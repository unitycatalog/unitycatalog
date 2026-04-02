package io.unitycatalog.client.delta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import io.unitycatalog.client.delta.model.DecimalType;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.PrimitiveType;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom deserializer for DeltaType that handles the string-or-object polymorphism in
 * StructField.type.
 */
public class DeltaTypeDeserializer extends StdDeserializer<DeltaType> {

  private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

  private final JsonDeserializer<?> defaultDeserializer;

  public DeltaTypeDeserializer() {
    super(DeltaType.class);
    this.defaultDeserializer = null;
  }

  public DeltaTypeDeserializer(JsonDeserializer<?> defaultDeserializer) {
    super(DeltaType.class);
    this.defaultDeserializer = defaultDeserializer;
  }

  @Override
  public DeltaType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    if (p.currentToken() == JsonToken.VALUE_STRING) {
      return parseString(p.getText());
    }
    return (DeltaType) defaultDeserializer.deserialize(p, ctxt);
  }

  @Override
  public DeltaType deserializeWithType(
      JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
      throws IOException {
    if (p.currentToken() == JsonToken.VALUE_STRING) {
      return parseString(p.getText());
    }
    return (DeltaType) typeDeserializer.deserializeTypedFromObject(p, ctxt);
  }

  private static DeltaType parseString(String s) {
    Matcher m = DECIMAL_PATTERN.matcher(s);
    if (m.matches()) {
      return new DecimalType()
          .precision(Integer.parseInt(m.group(1)))
          .scale(Integer.parseInt(m.group(2)));
    }
    return new PrimitiveType().type(s);
  }
}
