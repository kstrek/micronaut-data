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

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.type.Argument;
import io.micronaut.data.document.serde.IdPropertyNamingStrategy;
import io.micronaut.data.document.serde.IdSerializer;
import io.micronaut.data.document.serde.ManyRelationSerializer;
import io.micronaut.data.document.serde.OneRelationSerializer;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.Serde;
import io.micronaut.serde.SerdeIntrospections;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.support.DefaultSerdeRegistry;
import io.micronaut.serde.support.deserializers.ObjectDeserializer;
import io.micronaut.serde.support.serializers.ObjectSerializer;
import jakarta.inject.Singleton;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.IOException;

/**
 * Micronaut Data serde registry.
 *
 * @author Denis Stepanov
 * @since 3.3
 */
@Singleton
@Replaces(DefaultSerdeRegistry.class)
@Internal
final class DataSerdeRegistry extends DefaultSerdeRegistry {

    public static final IdPropertyNamingStrategy ID_PROPERTY_NAMING_STRATEGY = element -> "_id";

    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final AttributeConverterRegistry attributeConverterRegistry;

    /**
     * Default constructor.
     *
     * @param beanContext                The bean context
     * @param objectSerializer           The object serializer
     * @param objectDeserializer         The object deserializer
     * @param objectArraySerde           The objectArraySerde
     * @param introspections             The introspections
     * @param runtimeEntityRegistry      The runtimeEntityRegistry
     * @param attributeConverterRegistry The attributeConverterRegistry
     */
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
                                idSerializer.serialize(en, context, id, idType);
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
