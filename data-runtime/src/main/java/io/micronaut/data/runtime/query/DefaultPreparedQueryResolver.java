package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.annotation.Internal;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.StoredQuery;
import io.micronaut.data.operations.RepositoryOperations;
import io.micronaut.data.runtime.query.internal.DefaultPreparedQuery;

@Internal
public abstract class DefaultPreparedQueryResolver implements PreparedQueryResolver {

    @Override
    public <E, R> PreparedQuery<E, R> resolveQuery(MethodInvocationContext<?, ?> context,
                                                   StoredQuery<E, R> storedQuery,
                                                   Pageable pageable) {
        return new DefaultPreparedQuery<>(
                context,
                storedQuery,
                storedQuery.getQuery(),
                pageable,
                storedQuery.isDtoProjection(),
                getOperations().getConversionService()
        );
    }

    @Override
    public <E, R> PreparedQuery<E, R> resolveCountQuery(MethodInvocationContext<?, ?> context,
                                                        StoredQuery<E, R> storedQuery,
                                                        Pageable pageable) {
        return new DefaultPreparedQuery<>(
                context,
                storedQuery,
                storedQuery.getQuery(),
                pageable,
                false,
                getOperations().getConversionService()
        );
    }

    protected abstract RepositoryOperations getOperations();

}
