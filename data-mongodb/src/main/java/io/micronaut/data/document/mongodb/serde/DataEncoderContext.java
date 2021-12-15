package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.document.serde.CustomConverterSerializer;
import io.micronaut.data.document.serde.IdSerializer;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.bson.custom.CodecBsonDecoder;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.reference.PropertyReference;
import io.micronaut.serde.reference.SerializationReference;
import org.bson.codecs.Codec;
import org.bson.codecs.IterableCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.io.IOException;

public class DataEncoderContext implements Serializer.EncoderContext {

    private final Argument<ObjectId> OBJECT_ID = Argument.of(ObjectId.class);

    private final AttributeConverterRegistry attributeConverterRegistry;
    private final RuntimePersistentEntity<Object> runtimePersistentEntity;
    private final Serializer.EncoderContext parent;
    private final CodecRegistry codecRegistry;

    public DataEncoderContext(AttributeConverterRegistry attributeConverterRegistry,
                              RuntimePersistentEntity<Object> runtimePersistentEntity,
                              Serializer.EncoderContext parent,
                              CodecRegistry codecRegistry) {
        this.attributeConverterRegistry = attributeConverterRegistry;
        this.runtimePersistentEntity = runtimePersistentEntity;
        this.parent = parent;
        this.codecRegistry = codecRegistry;
    }

    @Override
    public ConversionService<?> getConversionService() {
        return parent.getConversionService();
    }

    @Override
    public boolean hasView(Class<?>... views) {
        return parent.hasView(views);
    }

    @Override
    public <B, P> SerializationReference<B, P> resolveReference(SerializationReference<B, P> reference) {
        return parent.resolveReference(reference);
    }

    @Override
    public <T, D extends Serializer<? extends T>> D findCustomSerializer(Class<? extends D> serializerClass) throws SerdeException {
        if (serializerClass == IdSerializer.class) {
            Serializer<? super ObjectId> objectIdSerializer = findSerializer(OBJECT_ID);
            IdSerializer idSerializer = new IdSerializer() {

                @Override
                public Serializer<Object> createSpecific(Argument<?> type, EncoderContext encoderContext) throws SerdeException {
                    if (type.getType() == String.class && type.isAnnotationPresent(GeneratedValue.class)) {
                        return (encoder, encoderContext2, value, stringType) -> {
                            String stringId = (String) value;
                            objectIdSerializer.serialize(encoder, encoderContext2, new ObjectId(stringId), OBJECT_ID);
                        };
                    }
                    return (Serializer<Object>) findSerializer(type);
                }

                @Override
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) throws IOException {
                    throw new IllegalStateException("Create specific call is required!");
                }
            };
            return (D) idSerializer;
        }
        if (serializerClass == CustomConverterSerializer.class) {
            CustomConverterSerializer customConverterSerializer = new CustomConverterSerializer() {
                @Override
                public Serializer<Object> createSpecific(Argument<?> type, EncoderContext encoderContext) throws SerdeException {
                    Class<?> converterClass = type.getAnnotationMetadata().classValue(MappedProperty.class, "converter").get();
                    Class<Object> converterPersistedType = type.getAnnotationMetadata().classValue(MappedProperty.class, "converterPersistedType").get();
                    Argument<Object> convertedType = Argument.of(converterPersistedType);
                    Serializer<? super Object> serializer = findSerializer(convertedType);
                    AttributeConverter<Object, Object> converter = attributeConverterRegistry.getConverter(converterClass);
                    return new Serializer<Object>() {

                        @Override
                        public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) throws IOException {
                            if (value == null) {
                                encoder.encodeNull();
                                return;
                            }
                            Object converted = converter.convertToPersistedValue(value, ConversionContext.of(type));
                            if (converted == null) {
                                encoder.encodeNull();
                                return;
                            }
                            serializer.serialize(encoder, context, converted, convertedType);
                        }

                    };
                }

                @Override
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) throws IOException {
                    throw new IllegalStateException("Create specific call is required!");
                }
            };
            return (D) customConverterSerializer;
        }
        return parent.findCustomSerializer(serializerClass);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Argument<? extends T> type) throws SerdeException {
        Codec<? extends T> codec = codecRegistry.get(type.getType(), codecRegistry);
        if (codec != null && !(codec instanceof IterableCodec)) {
            return new CodecBsonDecoder<T>((Codec<T>) codec) {

            };
        }
        return parent.findSerializer(type);
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
