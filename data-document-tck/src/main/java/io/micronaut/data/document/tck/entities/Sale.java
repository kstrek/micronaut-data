package io.micronaut.data.document.tck.entities;

import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;

@MappedEntity
public class Sale {
    //
//    @ManyToOne
//    private final Product product;
    @MappedProperty(converter = QuantityAttributeConverter.class)
    private Quantity quantity;

    @MappedProperty("_id")
    @Id
    @GeneratedValue
    private String id;

    public Quantity getQuantity() {
        return quantity;
    }

    public void setQuantity(Quantity quantity) {
        this.quantity = quantity;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
