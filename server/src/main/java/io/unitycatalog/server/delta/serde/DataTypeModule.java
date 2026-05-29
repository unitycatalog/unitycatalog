package io.unitycatalog.server.delta.serde;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import io.unitycatalog.server.delta.model.DataType;
import io.unitycatalog.server.delta.model.DecimalType;
import io.unitycatalog.server.delta.model.PrimitiveType;

/** Jackson module for DataType ser/deser. */
public class DataTypeModule extends SimpleModule {
  @SuppressWarnings("unchecked")
  public DataTypeModule() {
    super("DataTypeModule");
    setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          public JsonDeserializer<?> modifyDeserializer(
              DeserializationConfig config,
              BeanDescription desc,
              JsonDeserializer<?> deserializer) {
            if (desc.getBeanClass() == DataType.class) {
              return new DataTypeDeserializer(deserializer);
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
            if (cls == DataType.class || cls == PrimitiveType.class || cls == DecimalType.class) {
              return new DataTypeSerializer((JsonSerializer<Object>) serializer);
            }
            return serializer;
          }
        });
  }
}
