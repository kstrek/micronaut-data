package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.StoredQuery;

public interface PreparedQueryResolver {

    <E, R> PreparedQuery<E, R> resolveQuery(@NonNull MethodInvocationContext<?, ?> context,
                                            @NonNull StoredQuery<E, R> storedQuery,
                                            @NonNull Pageable pageable);

    <E, R> PreparedQuery<E, R> resolveCountQuery(@NonNull MethodInvocationContext<?, ?> context,
                                                 @NonNull StoredQuery<E, R> storedQuery,
                                               @Nullable Pageable pageable);

}
