package io.micronaut.data.document.mongodb.repositories;

import io.micronaut.data.document.mongodb.annotation.MongoDbRepository;
import io.micronaut.data.document.tck.repositories.SettlementRepository;

@MongoDbRepository
public interface MongoSettlementRepository extends SettlementRepository {
}
