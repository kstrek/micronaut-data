package io.micronaut.data.document.mongodb.database;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.PersistentEntity;

@Experimental
public final class SimpleMongoDatabaseFactory implements MongoDatabaseFactory {

    private final MongoClient mongoClient;
    private final String databaseName;

    public SimpleMongoDatabaseFactory(MongoClient mongoClient, String databaseName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
    }

    @Override
    public MongoDatabase getDatabase(PersistentEntity persistentEntity) throws DataAccessException {
        return mongoClient.getDatabase(databaseName);
    }

    @Override
    public MongoDatabase getDatabase(Class<?> entityClass) throws DataAccessException {
        return mongoClient.getDatabase(databaseName);
    }
}
