package io.micronaut.data.document.mongodb.database;

import com.mongodb.client.MongoDatabase;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.PersistentEntity;

@Experimental
public interface MongoDatabaseFactory {

    MongoDatabase getDatabase(PersistentEntity persistentEntity) throws DataAccessException;

    MongoDatabase getDatabase(Class<?> entityClass) throws DataAccessException;

}
