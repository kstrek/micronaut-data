/*
 * Copyright 2017-2022 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.document.serde.CustomConverterSerializer;
import io.micronaut.data.document.serde.IdPropertyNamingStrategy;
import io.micronaut.data.document.serde.IdSerializer;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.bson.custom.CodecBsonDecoder;
import io.micronaut.serde.config.naming.PropertyNamingStrategy;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.reference.PropertyReference;
import io.micronaut.serde.reference.SerializationReference;
import org.bson.codecs.Codec;
import org.bson.codecs.IterableCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * The Micronaut Data's Serde's {@link io.micronaut.serde.Serializer.EncoderContext}.
 *
 * @author Denis Stepanov
 * @since 3.3
 */
@Internal
final class DataEncoderContext implements Serializer.EncoderContext {

    private final Argument<ObjectId> OBJECT_ID = Argument.of(ObjectId.class);

    private final AttributeConverterRegistry attributeConverterRegistry;
    private final RuntimePersistentEntity<Object> runtimePersistentEntity;
    private final Serializer.EncoderContext parent;
    private final CodecRegistry codecRegistry;

    /**
     * Default constructor.
     *
     * @param attributeConverterRegistry The AttributeConverterRegistry
     * @param runtimePersistentEntity    The runtime persistent entity
     * @param parent                     The parent context
     * @param codecRegistry              The codec registry
     */
    DataEncoderContext(AttributeConverterRegistry attributeConverterRegistry,
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
            IdSerializer idSerializer = new IdSerializer() {

                @Override
                public Serializer<Object> createSpecific(Argument<?> type, EncoderContext encoderContext) throws SerdeException {
                    boolean isGeneratedObjectIdAsString = type.isAssignableFrom(String.class)
                            && type.isAnnotationPresent(GeneratedValue.class);
                    if (isGeneratedObjectIdAsString) {
                        Serializer<? super ObjectId> objectIdSerializer = findSerializer(OBJECT_ID);
                        return (encoder, encoderContext2, value, stringType) -> {
                            String stringId = (String) value;
                            objectIdSerializer.serialize(encoder, encoderContext2, new ObjectId(stringId), OBJECT_ID);
                        };
                    }
                    return (Serializer<Object>) findSerializer(type);
                }

                @Override
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) {
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
                public void serialize(Encoder encoder, EncoderContext context, Object value, Argument<?> type) {
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
            return new CodecBsonDecoder<T>((Codec<T>) codec);
        }
        return parent.findSerializer(type);
    }

    @Override
    public <D extends PropertyNamingStrategy> D findNamingStrategy(Class<? extends D> namingStrategyClass) throws SerdeException {
        if (namingStrategyClass == IdPropertyNamingStrategy.class) {
            return (D) DataSerdeRegistry.ID_PROPERTY_NAMING_STRATEGY;
        }
        return parent.findNamingStrategy(namingStrategyClass);
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
