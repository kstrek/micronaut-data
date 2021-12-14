package io.micronaut.data.document.tck.repositories;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.document.tck.entities.Person;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.repository.CrudRepository;
import io.micronaut.data.repository.jpa.JpaSpecificationExecutor;
import io.micronaut.data.repository.jpa.criteria.PredicateSpecification;

import java.util.List;

public interface PersonRepository extends CrudRepository<Person, String>, JpaSpecificationExecutor<Person> {

    Person get(String id);

    int count(String name);

    Person save(@Parameter("name") String name, @Parameter("age") int age);

    List<Person> list(Pageable pageable);

    long updateAll(List<Person> people);

    List<Person> updatePeople(List<Person> people);

    @Nullable
    Person findByName(String name);

    void updatePerson(@Id String id, @Parameter("name") String name);

    long updateByName(String name, int age);

    List<Person> findByNameRegex(String name);

    Long deleteByNameRegex(String name);

    long updatePersonCount(@Id String id, @Parameter("name") String name);

    int countByAgeGreaterThan(Integer wrapper);

    int countByAgeLessThan(int wrapper);

    int findAgeByName(String name);

    class Specifications {

        public static PredicateSpecification<Person> nameEquals(String name) {
            return (root, criteriaBuilder) -> criteriaBuilder.equal(root.get("name"), name);
        }

        public static PredicateSpecification<Person> nameEqualsCaseInsensitive(String name) {
            return (root, criteriaBuilder) -> criteriaBuilder.equal(criteriaBuilder.lower(root.get("name")), name.toLowerCase());
        }
    }
}
