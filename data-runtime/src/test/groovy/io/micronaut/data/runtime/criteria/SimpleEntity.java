package io.micronaut.data.runtime.criteria;

import io.micronaut.data.annotation.*;
import java.math.*;
import java.util.List;

@MappedEntity
class SimpleEntity {
    @Id
    @GeneratedValue(GeneratedValue.Type.IDENTITY)
    private Long id;
    private String name;
    private boolean enabled;
    private Boolean enabled2;
    private Long age;
    private BigDecimal amount;
    private BigDecimal budget;

    public SimpleEntity(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean getEnabled2() {
        return enabled2;
    }

    public void setEnabled2(Boolean enabled2) {
        this.enabled2 = enabled2;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getBudget() {
        return budget;
    }

    public void setBudget(BigDecimal budget) {
        this.budget = budget;
    }

}