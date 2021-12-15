package io.micronaut.data.document.mongodb.serde;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.type.Argument;
import io.micronaut.data.document.serde.IdSerializer;
import io.micronaut.data.document.serde.ManyRelationSerializer;
import io.micronaut.data.document.serde.OneRelationSerializer;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.DefaultSerdeRegistry;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.Serde;
import io.micronaut.serde.SerdeIntrospections;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.deserializers.ObjectDeserializer;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.serializers.ObjectSerializer;
import jakarta.inject.Singleton;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.IOException;

@Singleton
@Replaces(DefaultSerdeRegistry.class)
public class DataSerdeRegistry extends DefaultSerdeRegistry {

    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final AttributeConverterRegistry attributeConverterRegistry;

    public DataSerdeRegistry(BeanContext beanContext,
                             ObjectSerializer objectSerializer,
                             ObjectDeserializer objectDeserializer,
                             Serde<Object[]> objectArraySerde,
                             SerdeIntrospections introspections,
                             RuntimeEntityRegistry runtimeEntityRegistry,
                             AttributeConverterRegistry attributeConverterRegistry) {
        super(beanContext, objectSerializer, objectDeserializer, objectArraySerde, introspections);
        this.runtimeEntityRegistry = runtimeEntityRegistry;
        this.attributeConverterRegistry = attributeConverterRegistry;
    }

    public Serializer.EncoderContext newEncoderContext(Class<?> view, RuntimePersistentEntity<?> runtimePersistentEntity, CodecRegistry codecRegistry) {
        return new DataEncoderContext(attributeConverterRegistry, (RuntimePersistentEntity<Object>) runtimePersistentEntity, super.newEncoderContext(view), codecRegistry);
    }

    public Deserializer.DecoderContext newDecoderContext(Class<?> view, RuntimePersistentEntity<?> runtimePersistentEntity, CodecRegistry codecRegistry) {
        return new DataDecoderContext(attributeConverterRegistry, (RuntimePersistentEntity<Object>) runtimePersistentEntity, super.newDecoderContext(view), codecRegistry);
    }

    @Override
    public Serializer.EncoderContext newEncoderContext(Class<?> view) {
        return super.newEncoderContext(view);
    }

    @Override
    public Deserializer.DecoderContext newDecoderContext(Class<?> view) {
        return super.newDecoderContext(view);
    }

    @Override
    public <T, D extends Deserializer<? extends T>> D findCustomDeserializer(Class<? extends D> deserializerClass) throws SerdeException {
//        if (deserializerClass == OneRelationDeserializer.class) {
//            OneRelationDeserializer oneRelationDeserializer = new OneRelationDeserializer() {
//
//                @Override
//                public Object deserialize(Decoder decoder, DecoderContext decoderContext, Argument<? super Object> type) throws IOException {
//                    if (decoder.decodeNull()) {
//                        return null;
//                    }
//                    RuntimePersistentEntity entity = runtimeEntityRegistry.getEntity(type.getType());
//                    if (entity.getIdentity() == null) {
//                        throw new SerdeException("Cannot find ID of entity type: " + type);
//                    }
//                    Deserializer<Object> idDeserializer = findDeserializer(entity.getIdentity().getArgument());
//                    Object id = idDeserializer.deserialize(decoder, decoderContext, type);
//
//                    return entity.getIntrospection().instantiate();
//                }
//            };
//            return (D) oneRelationDeserializer;
//        }
        return super.findCustomDeserializer(deserializerClass);
    }

    @Override
    public <T, D extends Serializer<? extends T>> D findCustomSerializer(Class<? extends D> serializerClass) throws SerdeException {
        if (serializerClass == OneRelationSerializer.class) {
            OneRelationSerializer oneRelationSerializer = new OneRelationSerializer() {

                @Override
                public Serializer<Object> createSpecific(Argument<?> type, EncoderContext encoderContext) throws SerdeException {
                    RuntimePersistentEntity entity = runtimeEntityRegistry.getEntity(type.getType());
                    if (entity.getIdentity() == null) {
                        throw new SerdeException("Cannot find ID of entity type: " + type);
                    }
                    BeanProperty property = entity.getIdentity().getProperty();
                    Argument<?> idType = entity.getIdentity().getArgument();
                    Serializer<Object> idSerializer = encoderContext.findCustomSerializer(IdSerializer.class).createSpecific(idType, encoderContext);
                    return new Serializer<Object>() {
                        @Override
                        public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) throws IOException {
                            Object id = property.get(value);
                            if (id == null) {
                                encoder.encodeNull();
                            } else {
                                Encoder en = encoder.encodeObject(type);
                                en.encodeKey("_id");
                                idSerializer.serialize(encoder, context, id, idType);
                                en.finishStructure();
                            }
                        }
                    };
                }

                @Override
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<? extends Object> type) throws IOException {
                    throw new IllegalStateException("Create specific call is required!");
                }

            };
            return (D) oneRelationSerializer;
        }
        if (serializerClass == ManyRelationSerializer.class) {
            ManyRelationSerializer manyRelationSerializer = new ManyRelationSerializer() {

                @Override
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) throws IOException {
                    encoder.encodeNull();
                }
            };
            return (D) manyRelationSerializer;
        }
        return super.findCustomSerializer(serializerClass);
    }

}
