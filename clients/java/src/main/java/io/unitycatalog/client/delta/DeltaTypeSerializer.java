package io.unitycatalog.client.delta;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.unitycatalog.client.delta.model.DecimalType;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.PrimitiveType;
import java.io.IOException;

/**
 * Custom serializer for DeltaType that writes primitives and decimals as bare strings, and
 * delegates complex types to the default Jackson serializer.
 */
public class DeltaTypeSerializer extends StdSerializer<DeltaType> {

  private final JsonSerializer<Object> defaultSerializer;

  public DeltaTypeSerializer(JsonSerializer<Object> defaultSerializer) {
    super(DeltaType.class);
    this.defaultSerializer = defaultSerializer;
  }

  private static String toTypeString(DeltaType value) {
    if (value instanceof DecimalType) {
      DecimalType dt = (DecimalType) value;
      return "decimal(" + dt.getPrecision() + "," + dt.getScale() + ")";
    }
    if (value instanceof PrimitiveType) {
      return value.getType();
    }
    return null;
  }

  @Override
  public void serialize(DeltaType value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    String s = toTypeString(value);
    if (s != null) {
      gen.writeString(s);
    } else {
      defaultSerializer.serialize(value, gen, provider);
    }
  }

  @Override
  public void serializeWithType(
      DeltaType value, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer)
      throws IOException {
    String s = toTypeString(value);
    if (s != null) {
      gen.writeString(s);
    } else {
      defaultSerializer.serializeWithType(value, gen, provider, typeSer);
    }
  }
}
