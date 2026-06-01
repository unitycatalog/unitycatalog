package io.unitycatalog.client.delta.serde;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import io.unitycatalog.client.delta.model.DeltaDataType;
import io.unitycatalog.client.delta.model.DeltaDecimalType;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;

/** Jackson module for DeltaDataType ser/deser. */
public class DeltaTypeModule extends SimpleModule {
  @SuppressWarnings("unchecked")
  public DeltaTypeModule() {
    super("DeltaTypeModule");
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
