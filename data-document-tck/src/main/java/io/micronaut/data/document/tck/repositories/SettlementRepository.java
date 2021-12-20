package io.micronaut.data.document.tck.repositories;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.annotation.Join;
import io.micronaut.data.document.tck.entities.Settlement;
import io.micronaut.data.document.tck.entities.SettlementPk;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;
import java.util.Optional;

public interface SettlementRepository extends CrudRepository<Settlement, SettlementPk> {
    @Join(value = "settlementType")
    @Join(value = "zone")
    @Override
    Optional<Settlement> findById(@NonNull SettlementPk settlementPk);

    @Join(value = "settlementType")
    @Join(value = "zone")
    @Join(value = "id.county")
    Optional<Settlement> queryById(@NonNull SettlementPk settlementPk);

    @Join(value = "settlementType")
    @Join(value = "zone")
    @Join(value = "id.county")
    List<Settlement> findAll(Pageable pageable);
}
