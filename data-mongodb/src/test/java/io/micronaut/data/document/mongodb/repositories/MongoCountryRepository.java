package io.micronaut.data.document.mongodb.repositories;

import io.micronaut.data.document.mongodb.annotation.MongoDbRepository;
import io.micronaut.data.document.tck.repositories.CountryRepository;

@MongoDbRepository
public interface MongoCountryRepository extends CountryRepository {
}
