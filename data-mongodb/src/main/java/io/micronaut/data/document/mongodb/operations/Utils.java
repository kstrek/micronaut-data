package io.micronaut.data.document.mongodb.operations;

import com.mongodb.client.model.Filters;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.serde.config.annotation.SerdeConfig;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Date;

@Internal
public final class Utils {

    static Bson filterById(ConversionService<?> conversionService,
                           RuntimePersistentEntity<?> persistentEntity,
                           Object value) {
        RuntimePersistentProperty<?> identity = persistentEntity.getIdentity();
        if (identity != null) {
            AnnotationValue<BsonRepresentation> bsonRepresentation = identity.getAnnotationMetadata().getAnnotation(BsonRepresentation.class);
            if (bsonRepresentation != null) {
                BsonType bsonType = bsonRepresentation.getRequiredValue(BsonType.class);
                return Filters.eq(toBsonValue(conversionService, bsonType, value));
            } else {
                Class<?> type = identity.getProperty().getType();
                if (type == String.class) {
                    return Filters.eq(new ObjectId(value.toString()));
                }
                return Filters.eq(value);
            }
        }
        throw new IllegalStateException("Cannot determine id!");
    }

    static Bson filterByIdAndVersion(ConversionService<?> conversionService, RuntimePersistentEntity persistentEntity, Object entity) {
        if (persistentEntity.getIdentity() == null) {
            throw new IllegalStateException("Cannot determine id!");
        }
        Object idValue = persistentEntity.getIdentity().getProperty().get(entity);
        RuntimePersistentProperty version = persistentEntity.getVersion();
        if (version != null) {
            Object versionValue = version.getProperty().get(entity);
            return Filters.and(
                    filterById(conversionService, persistentEntity, idValue),
                    Filters.eq(getPropertyPersistName(version), versionValue)
            );
        }
        return filterById(conversionService, persistentEntity, idValue);
    }

    private static String getPropertyPersistName(PersistentProperty property) {
        return property.getAnnotationMetadata()
                .stringValue(SerdeConfig.class, SerdeConfig.PROPERTY)
                .orElseGet(property::getName);
    }

    static Object toValue(BsonValue bsonValue) {
        switch (bsonValue.getBsonType()) {
            case STRING:
                return bsonValue.asString().getValue();
            case INT32:
                return bsonValue.asInt32().getValue();
            case INT64:
                return bsonValue.asInt64().getValue();
            default:
                throw new IllegalStateException("Not implemented for: " + bsonValue.getBsonType());
        }
    }

    public static BsonValue toBsonValue(ConversionService<?> conversionService, Object value, CodecRegistry codecRegistry) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        if (value instanceof String) {
            return new BsonString((String) value);
        }
        if (value instanceof Integer) {
            return new BsonInt32((Integer) value);
        }
        if (value instanceof Long) {
            return new BsonInt64((Long) value);
        }
        if (value instanceof ObjectId) {
            return new BsonObjectId((ObjectId) value);
        }
        BsonDocument bsonDocument = BsonDocumentWrapper.asBsonDocument(value, codecRegistry).toBsonDocument();
        return bsonDocument;
//        throw new IllegalStateException("Not implemented for: " + value);
    }

    static BsonValue toBsonValue(ConversionService<?> conversionService, BsonType bsonType, Object value) {
        switch (bsonType) {
            case STRING:
                return new BsonString(value.toString());
            case OBJECT_ID:
                if (value instanceof String) {
                    return new BsonObjectId(new ObjectId((String) value));
                }
                if (value instanceof byte[]) {
                    return new BsonObjectId(new ObjectId((byte[]) value));
                }
                if (value instanceof Date) {
                    return new BsonObjectId(new ObjectId((Date) value));
                }
                return new BsonObjectId(conversionService.convertRequired(value, ObjectId.class));
            default:
                throw new IllegalStateException("Bson conversion to: " + bsonType + " is missing!");
        }
    }


}
