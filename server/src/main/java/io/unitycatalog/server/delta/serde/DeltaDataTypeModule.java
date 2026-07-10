package io.unitycatalog.server.delta.serde;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import io.unitycatalog.server.delta.model.DeltaDataType;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.serde.internal.DeltaTypeDeserializer;
import io.unitycatalog.server.delta.serde.internal.DeltaTypeSerializer;

/**
 * Jackson module that adds Delta's string-or-object wire format for {@link DeltaDataType}.
 *
 * <p>Register on any {@code ObjectMapper} that (de)serializes UC Delta API models:
 *
 * <pre>{@code
 * mapper.registerModule(new DeltaDataTypeModule());
 * }</pre>
 *
 * <p>Once registered, the mapper reads/writes bare strings (e.g. {@code "long"}, {@code
 * "decimal(10,2)"}) for {@link DeltaPrimitiveType} / {@link DeltaDecimalType}, while still using
 * the default object form for {@code array}, {@code map}, and {@code struct}.
 */
public class DeltaDataTypeModule extends SimpleModule {
  @SuppressWarnings("unchecked")
  public DeltaDataTypeModule() {
    super("DeltaDataTypeModule");
    setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          public JsonDeserializer<?> modifyDeserializer(
              DeserializationConfig config,
              BeanDescription desc,
              JsonDeserializer<?> deserializer) {
            if (desc.getBeanClass() == DeltaDataType.class) {
              return new DeltaTypeDeserializer(deserializer);
            }
            return deserializer;
          }
        });
    setSerializerModifier(
        new BeanSerializerModifier() {
          @Override
          public JsonSerializer<?> modifySerializer(
              SerializationConfig config, BeanDescription desc, JsonSerializer<?> serializer) {
            Class<?> cls = desc.getBeanClass();
            if (cls == DeltaDataType.class
                || cls == DeltaPrimitiveType.class
                || cls == DeltaDecimalType.class) {
              return new DeltaTypeSerializer((JsonSerializer<Object>) serializer);
            }
            return serializer;
          }
        });
  }
}
