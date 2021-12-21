package io.micronaut.data.document.tck.entities;

import io.micronaut.data.annotation.EmbeddedId;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.MappedProperty;

@MappedEntity("comp_country")
public class County {
    @EmbeddedId
    private CountyPk id;
    @MappedProperty
    private String countyName;
    @MappedProperty("is_enabled")
    private Boolean enabled;

    public CountyPk getId() {
        return id;
    }

    public void setId(CountyPk id) {
        this.id = id;
    }

    public String getCountyName() {
        return countyName;
    }

    public void setCountyName(String countyName) {
        this.countyName = countyName;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

}
