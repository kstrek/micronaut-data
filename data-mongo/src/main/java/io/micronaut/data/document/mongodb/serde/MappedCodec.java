package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.type.Argument;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.bson.BsonReaderDecoder;
import io.micronaut.serde.bson.BsonWriterEncoder;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.IOException;

public class MappedCodec<T> implements Codec<T> {

    protected final DataSerdeRegistry dataSerdeRegistry;
    protected final RuntimePersistentEntity<T> persistentEntity;
    protected final Class<T> type;
    protected final Argument<T> argument;
    protected final Serializer<? super T> serializer;
    protected final Deserializer<? extends T> deserializer;
    protected final CodecRegistry codecRegistry;

    public MappedCodec(DataSerdeRegistry dataSerdeRegistry,
                       RuntimePersistentEntity<T> persistentEntity,
                       Class<T> type,
                       CodecRegistry codecRegistry) {
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
}
