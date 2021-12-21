package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.beans.BeanProperty;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.document.mongodb.operations.Utils;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import org.bson.BsonValue;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

public class MappedEntityCodec<T> extends MappedCodec<T> implements CollectibleCodec<T> {

    private final boolean isGeneratedId;
    private final boolean isGeneratedObjectIdAsString;
    private final boolean isGeneratedObjectId;
    private final BeanProperty identityProperty;

    public MappedEntityCodec(DataSerdeRegistry dataSerdeRegistry,
                             RuntimePersistentEntity<T> persistentEntity,
                             Class<T> type,
                             CodecRegistry codecRegistry) {
        super(dataSerdeRegistry, persistentEntity, type, codecRegistry);
        RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
        if (identity == null) {
            throw new IllegalStateException("Identity not found!");
        }
        identityProperty = identity.getProperty();
        isGeneratedId = identity.isAnnotationPresent(GeneratedValue.class);
        isGeneratedObjectId = isGeneratedId && identity.getType().isAssignableFrom(ObjectId.class);
        isGeneratedObjectIdAsString = !isGeneratedObjectId && identity.getType().isAssignableFrom(String.class);
    }

    @Override
    public T generateIdIfAbsentFromDocument(T document) {
        if (isGeneratedId) {
            if (isGeneratedObjectId) {
                return (T) identityProperty.withValue(document, new ObjectId());
            } else if (isGeneratedObjectIdAsString) {
                return (T) identityProperty.withValue(document, new ObjectId().toHexString());
            }
            throw new IllegalStateException("Cannot generate id for entity: " + persistentEntity);
        }
        return document;
    }

    @Override
    public boolean documentHasId(T document) {
        return identityProperty.get(document) != null;
    }

    @Override
    public BsonValue getDocumentId(T document) {
        return Utils.idValue(null, persistentEntity, document, codecRegistry);
    }
}
