package io.micronaut.data.document.mongodb.repositories;

import io.micronaut.data.annotation.Join;
import io.micronaut.data.document.tck.repositories.AuthorRepository;
import io.micronaut.data.document.tck.repositories.BookRepository;
import io.micronaut.data.mongo.annotation.MongoRepository;
import org.bson.BsonDocument;

@MongoRepository
public abstract class MongoBookRepository extends BookRepository {

    public MongoBookRepository(AuthorRepository authorRepository) {
        super(authorRepository);
    }

    @Join("author.books")
    public abstract Iterable<BsonDocument> queryAll();
}
