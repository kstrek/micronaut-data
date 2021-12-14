package io.micronaut.data.document.tck.repositories;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.Join;
import io.micronaut.data.document.tck.entities.Author;
import io.micronaut.data.repository.CrudRepository;

import javax.validation.constraints.NotNull;
import java.util.Optional;

public interface AuthorRepository extends CrudRepository<Author, String> {

    Author findByName(String name);

    void updateNickname(@Id String id, @Parameter("nickName") @Nullable String nickName);

    @NonNull
    @Override
    @Join(value = "books")
    @Join(value = "books.pages")
    Optional<Author> findById(@NonNull @NotNull String id);

    @Join("books")
    Author queryByName(String name);

    Author findByBooksTitle(String title);

    @Join("books")
    Author searchByName(String name);
}
