package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.annotation.Internal;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.runtime.query.internal.DefaultPagedQuery;

@Internal
public class DefaultPagedQueryResolver implements PagedQueryResolver {

    @Override
    public <E> PagedQuery<E> resolveQuery(MethodInvocationContext<?, ?> context, Class<E> entityClass, Pageable pageable) {
        return new DefaultPagedQuery<>(context.getExecutableMethod(), entityClass, pageable);
    }

}
