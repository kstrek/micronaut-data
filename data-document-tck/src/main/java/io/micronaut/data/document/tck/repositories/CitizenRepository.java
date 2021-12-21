package io.micronaut.data.document.tck.repositories;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.annotation.Join;
import io.micronaut.data.document.tck.entities.Citizen;
import io.micronaut.data.repository.CrudRepository;

import java.util.Optional;

public interface CitizenRepository extends CrudRepository<Citizen, String> {
    @Join(value = "settlements.id.county")
    @Join(value = "settlements.zone")
    @Join(value = "settlements.settlementType")
    @Override
    Optional<Citizen> findById(@NonNull String id);

    Optional<Citizen> queryById(@NonNull String id);
}
