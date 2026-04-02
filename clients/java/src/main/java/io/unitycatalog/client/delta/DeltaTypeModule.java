package io.unitycatalog.client.delta;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import io.unitycatalog.client.delta.model.DecimalType;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.PrimitiveType;

/** Jackson module for DeltaType ser/deser. */
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
            if (desc.getBeanClass() == DeltaType.class) {
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
            if (cls == DeltaType.class || cls == PrimitiveType.class || cls == DecimalType.class) {
              return new DeltaTypeSerializer((JsonSerializer<Object>) serializer);
            }
            return serializer;
          }
        });
  }
}
