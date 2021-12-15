package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.runtime.PagedQuery;

public interface PagedQueryResolver {

    <E> PagedQuery<E> resolveQuery(@NonNull MethodInvocationContext<?, ?> context,
                                      @NonNull Class<E> entityClass,
                                      @NonNull Pageable pageable);

}
