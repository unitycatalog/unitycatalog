package io.unitycatalog.client.delta.serde.internal;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import io.unitycatalog.client.delta.model.DeltaDataType;
import io.unitycatalog.client.delta.model.DeltaDecimalType;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom deserializer for DeltaDataType that handles the string-or-object polymorphism in
 * DeltaStructField.type.
 */
public class DeltaTypeDeserializer extends StdDeserializer<DeltaDataType> {

  private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

  private final JsonDeserializer<?> defaultDeserializer;

  public DeltaTypeDeserializer(JsonDeserializer<?> defaultDeserializer) {
    super(DeltaDataType.class);
    this.defaultDeserializer = defaultDeserializer;
  }

  @Override
  public DeltaDataType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    if (p.currentToken() == JsonToken.VALUE_STRING) {
      return parseString(p.getText());
    }
    return (DeltaDataType) defaultDeserializer.deserialize(p, ctxt);
  }

  @Override
  public DeltaDataType deserializeWithType(
      JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
      throws IOException {
    if (p.currentToken() == JsonToken.VALUE_STRING) {
      return parseString(p.getText());
    }
    return (DeltaDataType) typeDeserializer.deserializeTypedFromObject(p, ctxt);
  }

  private static DeltaDataType parseString(String s) {
    Matcher m = DECIMAL_PATTERN.matcher(s);
    if (m.matches()) {
      return new DeltaDecimalType()
          .precision(Integer.parseInt(m.group(1)))
          .scale(Integer.parseInt(m.group(2)));
    }
    return new DeltaPrimitiveType().type(s);
  }
}
