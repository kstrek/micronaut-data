package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.core.annotation.Internal;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.intercept.annotation.DataMethod;
import io.micronaut.data.model.runtime.StoredQuery;
import io.micronaut.data.operations.RepositoryOperations;
import io.micronaut.data.runtime.query.internal.DefaultStoredQuery;

@Internal
public abstract class DefaultStoredQueryResolver implements StoredQueryResolver {

    @Override
    public <E, R> StoredQuery<E, R> resolveQuery(MethodInvocationContext<?, ?> context, Class<E> entityClass, Class<R> resultType) {
        if (resultType == null) {
            //noinspection unchecked
            resultType = (Class<R>) context.classValue(DataMethod.NAME, DataMethod.META_MEMBER_RESULT_TYPE)
                    .orElse(entityClass);
        }
        String query = context.stringValue(Query.class).orElseThrow(() ->
                new IllegalStateException("No query present in method")
        );
        return new DefaultStoredQuery<>(
                context.getExecutableMethod(),
                resultType,
                entityClass,
                query,
                false,
                getOperations()
        );
    }

    @Override
    public <E, R> StoredQuery<E, R> resolveCountQuery(MethodInvocationContext<?, ?> context, Class<E> entityClass, Class<R> resultType) {
        String query = context.stringValue(Query.class, DataMethod.META_MEMBER_COUNT_QUERY).orElseThrow(() ->
                new IllegalStateException("No query present in method")
        );
        return new DefaultStoredQuery<>(
                context.getExecutableMethod(),
                resultType,
                entityClass,
                query,
                true,
                getOperations()
        );
    }

    protected abstract RepositoryOperations getOperations();

}
