package io.micronaut.data.runtime.query;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.data.model.runtime.StoredQuery;

public interface StoredQueryResolver {

    <E, R> StoredQuery<E, R> resolveQuery(MethodInvocationContext<?, ?> context, Class<E> entityClass, Class<R> resultType);

    <E, R> StoredQuery<E, R> resolveCountQuery(MethodInvocationContext<?, ?> context, Class<E> entityClass, Class<R> resultType);

}
