package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.document.mongodb.operations.Utils;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.bson.BsonReaderDecoder;
import io.micronaut.serde.bson.BsonWriterEncoder;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.io.IOException;

public class DataCodec<T> implements CollectibleCodec<T> {

    private final DataSerdeRegistry dataSerdeRegistry;
    private final RuntimePersistentEntity<T> persistentEntity;
    private final Class<T> type;
    private final Argument<T> argument;
    private final Serializer<? super T> serializer;
    private final Deserializer<? extends T> deserializer;
    private final CodecRegistry codecRegistry;
    private final boolean isGeneratedObjectIdAsString;
    private final boolean isGeneratedObjectId;
    private final BeanProperty identityProperty;

    public DataCodec(DataSerdeRegistry dataSerdeRegistry, RuntimePersistentEntity<T> persistentEntity, Class<T> type, CodecRegistry codecRegistry) {
        this.dataSerdeRegistry = dataSerdeRegistry;
        this.persistentEntity = persistentEntity;
        this.type = type;
        this.argument = Argument.of(type);
        this.codecRegistry = codecRegistry;
        try {
            this.serializer = dataSerdeRegistry.findSerializer(argument);
            this.deserializer = dataSerdeRegistry.findDeserializer(argument);
        } catch (IOException e) {
            throw new DataAccessException("Cannot find serialize/deserializer for type: " + type + ". " + e.getMessage(), e);
        }
        identityProperty = persistentEntity.getIdentity().getProperty();
        isGeneratedObjectId = persistentEntity.getIdentity().getType().isAssignableFrom(Object.class);
        isGeneratedObjectIdAsString = !isGeneratedObjectId && persistentEntity.getIdentity().getType().isAssignableFrom(String.class)
                && persistentEntity.getIdentity().isAnnotationPresent(GeneratedValue.class);
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            T deserialize = deserializer.deserialize(new BsonReaderDecoder(reader), dataSerdeRegistry.newDecoderContext(type, persistentEntity, codecRegistry), argument);
            return deserialize;
        } catch (IOException e) {
            throw new DataAccessException("Cannot deserialize: " + type, e);
        }
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        try {
            serializer.serialize(new BsonWriterEncoder(writer), dataSerdeRegistry.newEncoderContext(type, persistentEntity, codecRegistry), value, argument);
        } catch (IOException e) {
            throw new DataAccessException("Cannot serialize: " + value, e);
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return type;
    }

    @Override
    public T generateIdIfAbsentFromDocument(T document) {
        if (isGeneratedObjectId) {
            return (T) identityProperty.withValue(document, new ObjectId());
        } else if (isGeneratedObjectIdAsString) {
            return (T) identityProperty.withValue(document, new ObjectId().toHexString());
        }
        throw new IllegalStateException("Cannot generate id for entity: " + persistentEntity);
    }

    @Override
    public boolean documentHasId(T document) {
        return identityProperty.get(document) != null;
    }

    @Override
    public BsonValue getDocumentId(T document) {
        Object id = identityProperty.get(document);
        return Utils.toBsonValue(null, id, codecRegistry);
    }
}
