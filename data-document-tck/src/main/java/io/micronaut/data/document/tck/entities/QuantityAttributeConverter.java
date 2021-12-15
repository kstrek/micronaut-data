
package io.micronaut.data.document.tck.entities;

import io.micronaut.core.convert.ConversionContext;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import jakarta.inject.Singleton;

@Singleton
public class QuantityAttributeConverter implements AttributeConverter<Quantity, Integer> {

    @Override
    public Integer convertToPersistedValue(Quantity quantity, ConversionContext context) {
        return quantity == null ? null : quantity.getAmount();
    }

    @Override
    public Quantity convertToEntityValue(Integer value, ConversionContext context) {
        return value == null ? null : Quantity.valueOf(value);
    }

}
