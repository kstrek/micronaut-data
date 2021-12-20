package io.micronaut.data.document.tck.repositories;

import io.micronaut.data.document.tck.entities.Zone;
import io.micronaut.data.repository.CrudRepository;

public interface ZoneRepository extends CrudRepository<Zone, Long> {
}
