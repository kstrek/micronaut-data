package io.micronaut.data.document.mongodb.database;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import io.micronaut.configuration.mongo.core.DefaultMongoConfiguration;
import io.micronaut.configuration.mongo.core.NamedMongoConfiguration;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;

@Internal
@Factory
final class MongoFactory {

    @Primary
    @Singleton
    MongoDatabaseFactory primaryMongoDatabaseFactory(DefaultMongoConfiguration mongoConfiguration, @Primary MongoClient mongoClient) {
        return mongoConfiguration.getConnectionString()
                .map(ConnectionString::getDatabase)
                .filter(StringUtils::isNotEmpty)
                .map(databaseName -> new SimpleMongoDatabaseFactory(mongoClient, databaseName))
                .orElseThrow(() -> new ConfigurationException("Please specify the default Mongo database in the url string"));
    }

    @EachBean(NamedMongoConfiguration.class)
    @Singleton
    MongoDatabaseFactory namedMongoDatabaseFactory(NamedMongoConfiguration mongoConfiguration, MongoClient mongoClient) {
        return mongoConfiguration.getConnectionString()
                .map(ConnectionString::getDatabase)
                .filter(StringUtils::isNotEmpty)
                .map(databaseName -> new SimpleMongoDatabaseFactory(mongoClient, databaseName))
                .orElseThrow(() -> new ConfigurationException("Please specify the default Mongo database in the url string"));
    }

}
