package io.micronaut.data.document.tck.repositories;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.document.tck.entities.Quantity;
import io.micronaut.data.document.tck.entities.Sale;
import io.micronaut.data.repository.CrudRepository;

public interface SaleRepository extends CrudRepository<Sale, String> {

    long updateQuantity(@Parameter("id") @Id String id, @Parameter("quantity") Quantity quantity);

}
