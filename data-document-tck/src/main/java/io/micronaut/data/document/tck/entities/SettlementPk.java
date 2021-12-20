package io.micronaut.data.document.tck.entities;

import io.micronaut.data.annotation.Embeddable;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.annotation.Relation;

@Embeddable
public class SettlementPk {
    @MappedProperty(value = "code")
    private String code;
    @MappedProperty(value = "code_id")
    private Integer codeId;
    @Relation(value = Relation.Kind.MANY_TO_ONE)
    private County county;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getCodeId() {
        return codeId;
    }

    public void setCodeId(Integer codeId) {
        this.codeId = codeId;
    }

    public County getCounty() {
        return county;
    }

    public void setCounty(County county) {
        this.county = county;
    }

}
