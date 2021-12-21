package io.micronaut.data.document.tck.entities;

import io.micronaut.data.annotation.EmbeddedId;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.annotation.Relation;

@MappedEntity("comp_settlement")
public class Settlement {

    @EmbeddedId
    private SettlementPk id;
    @MappedProperty
    private String description;
    @Relation(Relation.Kind.MANY_TO_ONE)
    private SettlementType settlementType;
    @Relation(Relation.Kind.MANY_TO_ONE)
    private Zone zone;
    @MappedProperty("is_enabled")
    private Boolean enabled;

    public SettlementPk getId() {
        return id;
    }

    public void setId(SettlementPk id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public SettlementType getSettlementType() {
        return settlementType;
    }

    public void setSettlementType(SettlementType settlementType) {
        this.settlementType = settlementType;
    }

    public Zone getZone() {
        return zone;
    }

    public void setZone(Zone zone) {
        this.zone = zone;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }
}
