package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.document.serde.CustomConverterDeserializer;
import io.micronaut.data.document.serde.IdDeserializer;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.serde.Decoder;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.bson.custom.CodecBsonDecoder;
import io.micronaut.serde.config.naming.PropertyNamingStrategy;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.reference.PropertyReference;
import org.bson.codecs.Codec;
import org.bson.codecs.IterableCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.Collection;

public class DataDecoderContext implements Deserializer.DecoderContext {

    private final Argument<ObjectId> OBJECT_ID = Argument.of(ObjectId.class);

    private final AttributeConverterRegistry attributeConverterRegistry;
    private final RuntimePersistentEntity<Object> runtimePersistentEntity;
    private final Deserializer.DecoderContext parent;
    private final CodecRegistry codecRegistry;

    public DataDecoderContext(AttributeConverterRegistry attributeConverterRegistry,
                              RuntimePersistentEntity<Object> runtimePersistentEntity,
                              Deserializer.DecoderContext parent,
                              CodecRegistry codecRegistry) {
        this.attributeConverterRegistry = attributeConverterRegistry;
        this.runtimePersistentEntity = runtimePersistentEntity;
        this.parent = parent;
        this.codecRegistry = codecRegistry;
    }

    @Override
    public <B, P> PropertyReference<B, P> resolveReference(PropertyReference<B, P> reference) {
        return parent.resolveReference(reference);
    }

    @Override
    public <T, D extends Deserializer<? extends T>> D findCustomDeserializer(Class<? extends D> deserializerClass) throws SerdeException {
        if (deserializerClass == IdDeserializer.class) {
            IdDeserializer idDeserializer = new IdDeserializer() {

                @Override
                public Deserializer<Object> createSpecific(Argument<? super Object> type, DecoderContext decoderContext) throws SerdeException {
                    if (type.getType().isAssignableFrom(String.class) && type.isAnnotationPresent(GeneratedValue.class)) {
                        Deserializer<? extends ObjectId> deserializer = findDeserializer(OBJECT_ID);
                        return (decoder, decoderContext2, objectIdType) -> {
                            ObjectId objectId = deserializer.deserialize(decoder, decoderContext2, OBJECT_ID);
                            return objectId == null ? null : objectId.toHexString();
                        };
                    }
                    return (Deserializer<Object>) findDeserializer(type);
                }

                @Override
                public Object deserialize(Decoder decoder, DecoderContext decoderContext, Argument<? super Object> type) throws IOException {
                    throw new IllegalStateException("Create specific call is required!");
                }
            };
            return (D) idDeserializer;
        }
        if (deserializerClass == CustomConverterDeserializer.class) {
            CustomConverterDeserializer customConverterDeserializer = new CustomConverterDeserializer() {

                @Override
                public Deserializer<Object> createSpecific(Argument<? super Object> type, DecoderContext decoderContext) throws SerdeException {
                    Class<?> converterClass = type.getAnnotationMetadata().classValue(MappedProperty.class, "converter").get();
                    Class<Object> converterPersistedType = type.getAnnotationMetadata().classValue(MappedProperty.class, "converterPersistedType").get();
                    Argument<Object> convertedType = Argument.of(converterPersistedType);
                    AttributeConverter<Object, Object> converter = attributeConverterRegistry.getConverter(converterClass);
                    Deserializer<?> deserializer = findDeserializer(convertedType);
                    return new Deserializer<Object>() {
                        @Override
                        public Object deserialize(Decoder decoder, DecoderContext decoderContext, Argument<? super Object> type) throws IOException {
                            if (decoder.decodeNull()) {
                                return null;
                            }
                            Object deserialized = deserializer.deserialize(decoder, decoderContext, convertedType);
                            return converter.convertToEntityValue(deserialized, ConversionContext.of(convertedType));
                        }
                    };
                }

                @Override
                public Object deserialize(Decoder decoder, DecoderContext decoderContext, Argument<? super Object> type) throws IOException {
                    throw new IllegalStateException("Create specific call is required!");
                }
            };
            return (D) customConverterDeserializer;
        }
        return parent.findCustomDeserializer(deserializerClass);
    }

    @Override
    public <T> Deserializer<? extends T> findDeserializer(Argument<? extends T> type) throws SerdeException {
        Codec<? extends T> codec = codecRegistry.get(type.getType(), codecRegistry);
        if (codec != null && !(codec instanceof IterableCodec)) {
            return new CodecBsonDecoder<T>((Codec<T>) codec) {

            };
        }
        return parent.findDeserializer(type);
    }

    @Override
    public <D extends PropertyNamingStrategy> D findNamingStrategy(Class<? extends D> namingStrategyClass) throws SerdeException {
        return parent.findNamingStrategy(namingStrategyClass);
    }

    @Override
    public <T> Collection<BeanIntrospection<? extends T>> getDeserializableSubtypes(Class<T> superType) {
        return parent.getDeserializableSubtypes(superType);
    }

    @Override
    public <B, P> void pushManagedRef(PropertyReference<B, P> reference) {
        parent.pushManagedRef(reference);
    }

    @Override
    public void popManagedRef() {
        parent.popManagedRef();
    }
}
