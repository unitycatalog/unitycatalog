package io.unitycatalog.client.delta.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.unitycatalog.client.delta.model.DeltaDataType;
import io.unitycatalog.client.delta.model.DeltaDecimalType;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;
import java.io.IOException;

/**
 * Custom serializer for DeltaDataType that writes primitives and decimals as bare strings, and
 * delegates complex types to the default Jackson serializer.
 */
public class DeltaTypeSerializer extends StdSerializer<DeltaDataType> {

  private final JsonSerializer<Object> defaultSerializer;

  public DeltaTypeSerializer(JsonSerializer<Object> defaultSerializer) {
    super(DeltaDataType.class);
    this.defaultSerializer = defaultSerializer;
  }

  private static String toTypeString(DeltaDataType value) {
    if (value instanceof DeltaDecimalType) {
      DeltaDecimalType dt = (DeltaDecimalType) value;
      return "decimal(" + dt.getPrecision() + "," + dt.getScale() + ")";
    }
    if (value instanceof DeltaPrimitiveType) {
      return value.getType();
    }
    return null;
  }

  @Override
  public void serialize(DeltaDataType value, JsonGenerator gen, SerializerProvider provider)
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
      DeltaDataType value, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSer)
      throws IOException {
    String s = toTypeString(value);
    if (s != null) {
      gen.writeString(s);
    } else {
      defaultSerializer.serializeWithType(value, gen, provider, typeSer);
    }
  }
}
