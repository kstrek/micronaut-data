package io.micronaut.data.document.tck.repositories;

import io.micronaut.data.document.tck.entities.County;
import io.micronaut.data.document.tck.entities.CountyPk;
import io.micronaut.data.repository.CrudRepository;

public interface CountryRepository extends CrudRepository<County, CountyPk> {
}
