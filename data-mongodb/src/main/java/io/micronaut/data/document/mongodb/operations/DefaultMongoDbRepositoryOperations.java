package io.micronaut.data.document.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.beans.BeanWrapper;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.Sort;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
import io.micronaut.data.model.runtime.InsertBatchOperation;
import io.micronaut.data.model.runtime.InsertOperation;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.UpdateBatchOperation;
import io.micronaut.data.model.runtime.UpdateOperation;
import io.micronaut.data.runtime.config.DataSettings;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntitiesOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntityOperations;
import io.micronaut.data.runtime.operations.internal.OperationContext;
import io.micronaut.data.runtime.operations.internal.SyncCascadeOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EachBean(MongoClient.class)
@Internal
public class DefaultMongoDbRepositoryOperations extends AbstractRepositoryOperations<ClientSession, Object>
        implements MongoDbRepositoryOperations, SyncCascadeOperations.SyncCascadeOperationsHelper<DefaultMongoDbRepositoryOperations.MongoDbOperationContext> {
    private static final Logger QUERY_LOG = DataSettings.QUERY_LOG;
    private final MongoClient mongoClient;
    private final SyncCascadeOperations<MongoDbOperationContext> cascadeOperations;
    private final BsonDocument EMPTY = new BsonDocument();

    /**
     * Default constructor.
     *
     * @param codecs                     The media type codecs
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     * @param mongoClient
     */
    protected DefaultMongoDbRepositoryOperations(List<MediaTypeCodec> codecs,
                                                 DateTimeProvider<Object> dateTimeProvider,
                                                 RuntimeEntityRegistry runtimeEntityRegistry,
                                                 DataConversionService<?> conversionService,
                                                 AttributeConverterRegistry attributeConverterRegistry,
                                                 MongoClient mongoClient) {
        super(codecs, dateTimeProvider, runtimeEntityRegistry, conversionService, attributeConverterRegistry);
        this.mongoClient = mongoClient;
        this.cascadeOperations = new SyncCascadeOperations<>(conversionService, this);
    }

    @Override
    public <T> T findOne(Class<T> type, Serializable id) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            Bson filter = Utils.filterById(conversionService, persistentEntity, id);
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
            }
            return getCollection(persistentEntity, type).find(clientSession, filter, type).first();
        }
    }

    @Override
    public <T, R> R findOne(PreparedQuery<T, R> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            Class<T> type = preparedQuery.getRootEntity();
            Class<R> resultType = preparedQuery.getResultType();
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            String query = preparedQuery.getQuery();
            Bson filter;
            List<BsonDocument> pipeline;
            if (query.startsWith("[")) {
                pipeline = getPipeline(preparedQuery, persistentEntity);
                filter = EMPTY;
            } else {
                pipeline = null;
                filter = getFilter(preparedQuery, persistentEntity);
            }
            if (preparedQuery.isCount()) {
                return getCount(clientSession, type, resultType, persistentEntity, filter, pipeline);
            }
            if (pipeline == null) {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
                }
                return getCollection(persistentEntity, resultType)
                        .find(clientSession, filter, resultType)
                        .limit(1)
                        .map(r -> {
                            if (type.isInstance(r)) {
                                return (R) triggerPostLoad(preparedQuery.getAnnotationMetadata(), persistentEntity, type.cast(r));
                            }
                            return r;
                        })
                        .first();
            } else {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
                }
                boolean isProjection = pipeline.stream().anyMatch(stage -> stage.containsKey("$groupId") || stage.containsKey("$project"));
                if (isProjection) {
                    BsonDocument result = getCollection(persistentEntity, BsonDocument.class)
                            .aggregate(clientSession, pipeline, BsonDocument.class)
                            .first();
                    return convertResult(resultType, result);
                }
                return getCollection(persistentEntity, resultType)
                        .aggregate(clientSession, pipeline)
                        .map(r -> {
                            if (type.isInstance(r)) {
                                return (R) triggerPostLoad(preparedQuery.getAnnotationMetadata(), persistentEntity, type.cast(r));
                            }
                            return r;
                        })
                        .first();
            }
        }
    }

    private <R> R convertResult(Class<R> resultType, BsonDocument result) {
        BsonValue value;
        if (result == null) {
            value = BsonNull.VALUE;
        } else if (result.size() == 1) {
            value = result.values().iterator().next().asNumber();
        } else if (result.size() == 2) {
            value = result.entrySet().stream().filter(f -> !f.getKey().equals("_id")).findFirst().get().getValue();
        } else {
            throw new IllegalStateException();
        }
        return conversionService.convertRequired(Utils.toValue(value), resultType);
    }

    @Override
    public <T> boolean exists(PreparedQuery<T, Boolean> preparedQuery) {
        Class<T> type = preparedQuery.getRootEntity();
        RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
        Bson filter = getFilter(preparedQuery, persistentEntity);
        try (ClientSession clientSession = mongoClient.startSession()) {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing exists Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
            }
            return getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                    .find(clientSession, type)
                    .limit(1)
                    .filter(filter)
                    .iterator()
                    .hasNext();
        }
    }

    private <K> K triggerPostLoad(AnnotationMetadata annotationMetadata, RuntimePersistentEntity<K> persistentEntity, K entity) {
        if (persistentEntity.hasPostLoadEventListeners()) {
            entity = triggerPostLoad(entity, persistentEntity, annotationMetadata);
        }
        for (PersistentProperty pp : persistentEntity.getPersistentProperties()) {
            if (pp instanceof RuntimeAssociation) {
                RuntimeAssociation runtimeAssociation = (RuntimeAssociation) pp;
                Object o = runtimeAssociation.getProperty().get(entity);
                if (o == null) {
                    continue;
                }
                RuntimePersistentEntity associatedEntity = runtimeAssociation.getAssociatedEntity();
                switch (runtimeAssociation.getKind()) {
                    case MANY_TO_MANY:
                    case ONE_TO_MANY:
                        if (o instanceof Iterable) {
                            Iterable iterable = ((Iterable) o);
                            Iterator iterator = iterable.iterator();
                            while (iterator.hasNext()) {
                                triggerPostLoad(iterator.next(), associatedEntity, annotationMetadata);
                            }
                        }
                        continue;
                    case MANY_TO_ONE:
                    case ONE_TO_ONE:
                    case EMBEDDED:
                        triggerPostLoad(o, associatedEntity, annotationMetadata);
                        continue;
                }
            }
        }
        return entity;
    }


    private <T> Bson getUpdate(PreparedQuery<?, ?> preparedQuery, RuntimePersistentEntity<T> persistentEntity) {
        String query = preparedQuery.getAnnotationMetadata().stringValue(Query.class, "update")
                .orElseThrow(() -> new IllegalArgumentException("Update query is not provided!"));
        return getQuery(preparedQuery, persistentEntity, query);
    }

    private <T> Bson getFilter(PreparedQuery<?, ?> preparedQuery, RuntimePersistentEntity<T> persistentEntity) {
        String query = preparedQuery.getQuery();
        return getQuery(preparedQuery, persistentEntity, query);
    }

    private <T> List<BsonDocument> getPipeline(PreparedQuery<?, ?> preparedQuery, RuntimePersistentEntity<T> persistentEntity) {
        String query = preparedQuery.getQuery();
        BsonArray bsonArray = BsonArray.parse(query);
        bsonArray = (BsonArray) replaceQueryParameters(preparedQuery, persistentEntity, bsonArray);
        return bsonArray.stream().map(BsonValue::asDocument).collect(Collectors.toList());
    }

    private <T> BsonDocument getQuery(PreparedQuery<?, ?> preparedQuery, RuntimePersistentEntity<T> persistentEntity, String query) {
        if (StringUtils.isEmpty(query)) {
            return EMPTY;
        }
        BsonDocument bsonDocument = BsonDocument.parse(query);
        bsonDocument = (BsonDocument) replaceQueryParameters(preparedQuery, persistentEntity, bsonDocument);
        return bsonDocument;
    }

    private <T> BsonValue replaceQueryParameters(PreparedQuery<?, ?> preparedQuery, RuntimePersistentEntity<T> persistentEntity, BsonValue value) {
        if (value instanceof BsonDocument) {
            BsonDocument bsonDocument = (BsonDocument) value;
            BsonInt32 queryParameterIndex = bsonDocument.getInt32("$qpidx", null);
            if (queryParameterIndex != null) {
                int index = queryParameterIndex.getValue();
                return getValue(index, preparedQuery.getQueryBindings().get(index), preparedQuery, persistentEntity);
            }

            for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
                BsonValue bsonValue = entry.getValue();
                BsonValue newValue = replaceQueryParameters(preparedQuery, persistentEntity, bsonValue);
                if (bsonValue != newValue) {
                    entry.setValue(newValue);
                }
            }
            return bsonDocument;
        } else if (value instanceof BsonArray) {
            BsonArray bsonArray = (BsonArray) value;
            for (int i = 0; i < bsonArray.size(); i++) {
                BsonValue bsonValue = bsonArray.get(i);
                BsonValue newValue = replaceQueryParameters(preparedQuery, persistentEntity, bsonValue);
                if (bsonValue != newValue) {
                    if (newValue.isNull()) {
                        bsonArray.remove(i);
                        i -= 1;
                    } else if (newValue.isArray()) {
                        bsonArray.remove(i);
                        List<BsonValue> values = newValue.asArray().getValues();
                        bsonArray.addAll(i, values);
                        i += values.size() - 1;
                    } else {
                        bsonArray.set(i, newValue);
                    }
                }
            }
        }
        return value;
    }

    private <T> BsonValue getValue(int index,
                                   QueryParameterBinding queryParameterBinding,
                                   PreparedQuery<?, ?> preparedQuery,
                                   RuntimePersistentEntity<T> persistentEntity) {
        Class<?> parameterConverter = queryParameterBinding.getParameterConverterClass();
        Object value;
        if (queryParameterBinding.getParameterIndex() != -1) {
            value = resolveParameterValue(queryParameterBinding, preparedQuery.getParameterArray());
        } else if (queryParameterBinding.isAutoPopulated()) {
            PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
            RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
            Object previousValue = null;
            QueryParameterBinding previousPopulatedValueParameter = queryParameterBinding.getPreviousPopulatedValueParameter();
            if (previousPopulatedValueParameter != null) {
                if (previousPopulatedValueParameter.getParameterIndex() == -1) {
                    throw new IllegalStateException("Previous value parameter cannot be bind!");
                }
                previousValue = resolveParameterValue(previousPopulatedValueParameter, preparedQuery.getParameterArray());
            }
            value = runtimeEntityRegistry.autoPopulateRuntimeProperty(persistentProperty, previousValue);
//                value = context.convert(connection, value, persistentProperty);
            parameterConverter = null;
        } else {
            throw new IllegalStateException("Invalid query [" + "]. Unable to establish parameter value for parameter at position: " + (index + 1));
        }

        DataType dataType = queryParameterBinding.getDataType();
        List<Object> values = expandValue(value, dataType);
        if (values != null && values.isEmpty()) {
            // Empty collections / array should always set at least one value
            value = null;
            values = null;
        }
        if (values == null) {
            if (parameterConverter != null) {
                int parameterIndex = queryParameterBinding.getParameterIndex();
                Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
//                    value = context.convert(parameterConverter, connection, value, argument);
            }
            if (value instanceof String) {
                PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
                RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
                if (persistentProperty instanceof RuntimeAssociation) {
                    RuntimeAssociation runtimeAssociation = (RuntimeAssociation) persistentProperty;
                    RuntimePersistentProperty identity = runtimeAssociation.getAssociatedEntity().getIdentity();
                    if (identity != null && identity.getType() == String.class && identity.isGenerated()) {
                        return new BsonObjectId(new ObjectId((String) value));
                    }
                }
                if (persistentProperty.getOwner().getIdentity() == persistentProperty && persistentProperty.getType() == String.class && persistentProperty.isGenerated()) {
                    return new BsonObjectId(new ObjectId((String) value));
                }
            }
            return Utils.toBsonValue(conversionService, value);
        } else {
            return new BsonArray(
                    values.stream().map(val -> Utils.toBsonValue(conversionService, val)).collect(Collectors.toList())
            );
//            Iterator<Object> iterator = values.iterator();
//            while (iterator.hasNext()) {
//                if (parameterConverter != null) {
//                    int parameterIndex = queryParameterBinding.getParameterIndex();
//                    Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
////                        v = context.convert(parameterConverter, connection, v, argument);
//                }
//                appendValue(q, iterator.next());
//                if (iterator.hasNext()) {
//                    q.append(", ");
//                }
////                    context.setStatementParameter(stmt, index++, dataType, v, dialect);
//            }
        }
    }

    private <T> PersistentPropertyPath getRequiredPropertyPath(QueryParameterBinding queryParameterBinding, RuntimePersistentEntity<T> persistentEntity) {
        String[] propertyPath = queryParameterBinding.getRequiredPropertyPath();
        PersistentPropertyPath pp = persistentEntity.getPropertyPath(propertyPath);
        if (pp == null) {
            throw new IllegalStateException("Cannot find auto populated property: " + String.join(".", propertyPath));
        }
        return pp;
    }

    List<Object> expandValue(Object value, DataType dataType) {
        // Special case for byte array, we want to support a list of byte[] convertible values
        if (value == null || dataType != null && dataType.isArray() && dataType != DataType.BYTE_ARRAY || value instanceof byte[]) {
            // not expanded
            return null;
        } else if (value instanceof Iterable) {
            return (List<Object>) CollectionUtils.iterableToList((Iterable<?>) value);
        } else if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            if (len == 0) {
                return Collections.emptyList();
            } else {
                List<Object> list = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    Object o = Array.get(value, j);
                    list.add(o);
                }
                return list;
            }
        } else {
            // not expanded
            return null;
        }
    }

    private Object resolveParameterValue(QueryParameterBinding queryParameterBinding, Object[] parameterArray) {
        Object value;
        value = parameterArray[queryParameterBinding.getParameterIndex()];
        String[] parameterBindingPath = queryParameterBinding.getParameterBindingPath();
        if (parameterBindingPath != null) {
            for (String prop : parameterBindingPath) {
                if (value == null) {
                    break;
                }
                value = BeanWrapper.getWrapper(value).getRequiredProperty(prop, Argument.OBJECT_ARGUMENT);
            }
        }
        return value;
    }

    @Override
    public <T> Iterable<T> findAll(PagedQuery<T> query) {
        return null;
    }

    @Override
    public <T> long count(PagedQuery<T> pagedQuery) {
        return 0;
    }

    @Override
    public <T, R> Iterable<R> findAll(PreparedQuery<T, R> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            Pageable pageable = preparedQuery.getPageable();

            Class<T> type = preparedQuery.getRootEntity();
            Class<R> resultType = preparedQuery.getResultType();
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);

            String query = preparedQuery.getQuery();
            Bson filter;
            List<BsonDocument> pipeline;
            if (query.startsWith("[")) {
                pipeline = getPipeline(preparedQuery, persistentEntity);
                filter = EMPTY;
            } else {
                pipeline = null;
                filter = getFilter(preparedQuery, persistentEntity);
            }

            if (preparedQuery.isCount()) {
                return Collections.singletonList(getCount(clientSession, type, resultType, persistentEntity, filter, pipeline));
            }
            if (pipeline == null) {
                Bson sort = null;
                int skip = 0;
                int limit = 0;
                boolean isSingleResult = false;
                if (pageable != Pageable.UNPAGED) {
                    skip = (int) pageable.getOffset();
                    limit = pageable.getSize();
                    Sort pageableSort = pageable.getSort();
                    if (pageableSort.isSorted()) {
                        sort = pageableSort.getOrderBy().stream()
                                .map(order -> order.isAscending() ? Sorts.ascending(order.getProperty()) : Sorts.descending(order.getProperty()))
                                .collect(Collectors.collectingAndThen(Collectors.toList(), Sorts::orderBy));


                    }
                    if (isSingleResult && pageable.getOffset() > 0) {
                        pageable = Pageable.from(pageable.getNumber(), 1);
                    }
                }
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'find' with filter: {} skip: {} limit: {}", filter.toBsonDocument().toJson(), skip, limit);
                }
                return getCollection(persistentEntity, resultType)
                        .find(clientSession, filter, resultType)
                        .skip(skip)
                        .limit(limit)
                        .sort(sort)
                        .into(new ArrayList<>(limit > 0 ? limit : 20));
            }
            boolean isSingleResult = false;
            int limit = 0;
            if (pageable != Pageable.UNPAGED) {
                if (isSingleResult && pageable.getOffset() > 0) {
                    pageable = Pageable.from(pageable.getNumber(), 1);
                }
                int skip = (int) pageable.getOffset();
                limit = pageable.getSize();
                Sort pageableSort = pageable.getSort();
                if (pageableSort.isSorted()) {
                    Bson sort = pageableSort.getOrderBy().stream()
                            .map(order -> order.isAscending() ? Sorts.ascending(order.getProperty()) : Sorts.descending(order.getProperty()))
                            .collect(Collectors.collectingAndThen(Collectors.toList(), Sorts::orderBy));
                    addStageToPipelineBefore(pipeline, sort.toBsonDocument(), "$limit", "$skip");
                }
                if (skip > 0) {
                    pipeline.add(new BsonDocument().append("$skip", new BsonInt32(skip)));
                }
                if (limit > 0) {
                    pipeline.add(new BsonDocument().append("$limit", new BsonInt32(limit)));
                }
            }
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
            }
            return getCollection(persistentEntity, resultType)
                    .aggregate(clientSession, pipeline, resultType)
                    .into(new ArrayList<>(limit > 0 ? limit : 20));
        }
    }

    private <T, R> R getCount(ClientSession clientSession, Class<T> type, Class<R> resultType, RuntimePersistentEntity<T> persistentEntity, Bson filter, List<BsonDocument> pipeline) {
        if (pipeline == null) {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'countDocuments' with filter: {}", filter.toBsonDocument().toJson());
            }
            long count = getCollection(persistentEntity, BsonDocument.class).countDocuments(clientSession, filter);
            return conversionService.convertRequired(count, resultType);
        } else {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
            }
            R result = getCollection(persistentEntity, type)
                    .aggregate(clientSession, pipeline, BsonDocument.class)
                    .map(bsonDocument -> convertResult(resultType, bsonDocument))
                    .first();
            if (result == null) {
                result = conversionService.convertRequired(0, resultType);
            }
            return result;
        }
    }

    private void addStageToPipelineBefore(List<BsonDocument> pipeline, BsonDocument stageToAdd, String... beforeStages) {
        int lastFoundIndex = -1;
        int index = 0;
        for (Iterator<BsonDocument> iterator = pipeline.iterator(); iterator.hasNext(); ) {
            BsonDocument stage = iterator.next();
            for (String beforeStageName : beforeStages) {
                if (stage.containsKey(beforeStageName)) {
                    lastFoundIndex = index;
                    break;
                }
            }
            index++;
        }
        if (lastFoundIndex > -1) {
            pipeline.add(lastFoundIndex, stageToAdd);
        } else {
            pipeline.add(stageToAdd);
        }
    }

    @Override
    public <T, R> Stream<R> findStream(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> Stream<T> findStream(PagedQuery<T> query) {
        return null;
    }

    @Override
    public <R> Page<R> findPage(PagedQuery<R> query) {
        return null;
    }

    @Override
    public <T> T persist(InsertOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> Iterable<T> persistAll(InsertBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()), null);
        }
    }

    @Override
    public <T> T update(UpdateOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> Iterable<T> updateAll(UpdateBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> int delete(DeleteOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbEntityOperation<T> op = createMongoDbDeleteOneOperation(ctx, persistentEntity, operation.getEntity());
            op.delete();
            return (int) op.modifiedCount;
        }
    }

    @Override
    public <T> Optional<Number> deleteAll(DeleteBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            if (operation.all()) {
                RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
                long deletedCount = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .deleteMany(EMPTY)
                        .getDeletedCount();
                return Optional.of(deletedCount);
            }
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbEntitiesOperation<T> op = createMongoDbDeleteManyOperation(ctx, persistentEntity, operation);
            op.delete();
            return Optional.of(op.modifiedCount);
        }
    }

    @Override
    public Optional<Number> executeUpdate(PreparedQuery<?, Number> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            RuntimePersistentEntity<?> persistentEntity = runtimeEntityRegistry.getEntity(preparedQuery.getRootEntity());
            Bson update = getUpdate(preparedQuery, persistentEntity);
            Bson filter = getFilter(preparedQuery, persistentEntity);
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'updateMany' with filter: {} and update: {}", filter.toBsonDocument().toJson(), update.toBsonDocument().toJson());
            }
            UpdateResult updateResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                    .updateMany(clientSession, filter, update);
            if (preparedQuery.isOptimisticLock()) {
                checkOptimisticLocking(1, (int) updateResult.getModifiedCount());
            }
            return Optional.of(updateResult.getModifiedCount());
        }
    }

    public <T> Map<QueryParameterBinding, Object> collectAutoPopulatedPreviousValues(PreparedQuery<?, ?> preparedQuery,
                                                                                     RuntimePersistentEntity<T> persistentEntity,
                                                                                     T entity) {
        List<QueryParameterBinding> queryBindings = preparedQuery.getQueryBindings();
        if (queryBindings.isEmpty()) {
            return null;
        }
        return queryBindings.stream()
                .filter(b -> b.isAutoPopulated() && b.isRequiresPreviousPopulatedValue())
                .map(b -> {
                    if (b.getPropertyPath() == null) {
                        throw new IllegalStateException("Missing property path for query parameter: " + b);
                    }
                    Object value = entity;
                    for (String property : b.getPropertyPath()) {
                        if (value == null) {
                            break;
                        }
                        value = BeanWrapper.getWrapper(value).getRequiredProperty(property, Argument.OBJECT_ARGUMENT);
                    }
                    return new AbstractMap.SimpleEntry<>(b, value);
                })
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    @Override
    public Optional<Number> executeDelete(PreparedQuery<?, Number> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            RuntimePersistentEntity<?> persistentEntity = runtimeEntityRegistry.getEntity(preparedQuery.getRootEntity());
            Bson filter = getFilter(preparedQuery, persistentEntity);
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'deleteMany' with filter: {}", filter.toBsonDocument().toJson());
            }
            DeleteResult deleteResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                    .deleteMany(clientSession, filter);
            if (preparedQuery.isOptimisticLock()) {
                checkOptimisticLocking(1, (int) deleteResult.getDeletedCount());
            }
            return Optional.of(deleteResult.getDeletedCount());
        }
    }

    @Override
    protected ConversionContext createTypeConversionContext(ClientSession connection, RuntimePersistentProperty<?> property, Argument<?> argument) {
        return null;
    }

    @Override
    public void setStatementParameter(Object preparedStatement, int index, DataType dataType, Object value, Dialect dialect) {

    }

    private <T, R> MongoCollection<R> getCollection(RuntimePersistentEntity<T> persistentEntity, Class<R> resultType) {
        return getDatabase().getCollection(persistentEntity.getPersistedName(), resultType);
    }

    private MongoDatabase getDatabase() {
        return mongoClient.getDatabase("default");
    }

    @Override
    public boolean supportsBatch(MongoDbOperationContext mongoDbOperationContext, RuntimePersistentEntity<?> persistentEntity) {
        return true;
    }

    @Override
    public <T> T persistOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntityOperation<T> op = createMongoDbInsertOneOperation(ctx, persistentEntity, value);
        op.persist();
        return op.getEntity();
    }

    @Override
    public <T> List<T> persistBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity, Predicate<T> predicate) {
        MongoDbEntitiesOperation<T> op = createMongoDbInsertManyOperation(ctx, persistentEntity, values);
        if (predicate != null) {
            op.veto(predicate);
        }
        op.persist();
        return op.getEntities();
    }

    @Override
    public <T> T updateOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntityOperation<T> op = createMongoDbReplaceOneOperation(ctx, persistentEntity, value);
        op.update();
        return op.getEntity();
    }

    private <T> List<T> updateBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntitiesOperation<T> op = createMongoDbReplaceManyOperation(ctx, persistentEntity, values);
        op.update();
        return op.getEntities();
    }

    @Override
    public void persistManyAssociation(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Object child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    @Override
    public void persistManyAssociationBatch(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    private <T> MongoDbEntityOperation<T> createMongoDbInsertOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, true) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertOne' with entity: {}", entity);
                }
                InsertOneResult insertOneResult = collection.insertOne(ctx.clientSession, entity);
                BsonValue insertedId = insertOneResult.getInsertedId();
                BeanProperty<T, Object> property = (BeanProperty<T, Object>) persistentEntity.getIdentity().getProperty();
                if (property.get(entity) == null) {
                    entity = updateEntityId(property, entity, insertedId);
                }
            }
        };
    }

    private <T> MongoDbEntityOperation<T> createMongoDbReplaceOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, false) {

            Bson filter;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filter = Utils.filterByIdAndVersion(conversionService, persistentEntity, entity);
            }

            @Override
            protected void execute() throws RuntimeException {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                BsonDocument bsonDocument = BsonDocumentWrapper.asBsonDocument(entity, getDatabase().getCodecRegistry());
                bsonDocument.remove("_id");
                UpdateResult updateResult = getCollection(persistentEntity, BsonDocument.class)
                        .replaceOne(ctx.clientSession, filter, bsonDocument);
                modifiedCount = updateResult.getModifiedCount();
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(1, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbReplaceManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            Map<Data, Bson> filters;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filters = entities.stream()
                        .collect(Collectors.toMap(d -> d, d -> Utils.filterByIdAndVersion(conversionService, persistentEntity, d.entity)));
            }

            @Override
            protected void execute() throws RuntimeException {
                int expectedToBeUpdated = 0;
                for (Data d : entities) {
                    if (d.vetoed) {
                        continue;
                    }
                    expectedToBeUpdated++;
                    Bson filter = filters.get(d);
                    if (QUERY_LOG.isDebugEnabled()) {
                        QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                    }
                    BsonDocument bsonDocument = BsonDocumentWrapper.asBsonDocument(d.entity, getDatabase().getCodecRegistry());
                    bsonDocument.remove("_id");
                    UpdateResult updateResult = getCollection(persistentEntity, BsonDocument.class)
                            .replaceOne(ctx.clientSession, filter, bsonDocument);
                    modifiedCount += updateResult.getModifiedCount();
                }
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(expectedToBeUpdated, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoDbEntityOperation<T> createMongoDbDeleteOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, false) {

            Bson filter;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filter = Utils.filterByIdAndVersion(conversionService, persistentEntity, entity);
            }

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'deleteOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                DeleteResult deleteResult = collection.deleteOne(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(1, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbDeleteManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            Map<Data, Bson> filters;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filters = entities.stream()
                        .collect(Collectors.toMap(d -> d, d -> Utils.filterByIdAndVersion(conversionService, persistentEntity, d.entity)));
            }

            @Override
            protected void execute() throws RuntimeException {
                Bson filter = entities.stream()
                        .filter(d -> !d.vetoed)
                        .map(d -> filters.get(d))
                        .collect(Collectors.collectingAndThen(Collectors.toList(), Filters::or));
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'deleteMany' with filter: {}", filter.toBsonDocument().toJson());
                }
                DeleteResult deleteResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .deleteMany(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
                if (persistentEntity.getVersion() != null) {
                    int expected = (int) entities.stream().filter(d -> !d.vetoed).count();
                    checkOptimisticLocking(expected, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbInsertManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, true) {

            @Override
            protected void execute() throws RuntimeException {
                List<T> toInsert = entities.stream().map(d -> d.entity).collect(Collectors.toList());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertMany' with entities: {}", toInsert);
                }
                InsertManyResult insertManyResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .insertMany(toInsert);
                if (hasGeneratedId) {
                    Map<Integer, BsonValue> insertedIds = insertManyResult.getInsertedIds();
                    RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                    BeanProperty<T, Object> IdProperty = (BeanProperty<T, Object>) identity.getProperty();
                    int index = 0;
                    for (Data d : entities) {
                        if (!d.vetoed) {
                            BsonValue id = insertedIds.get(index);
                            if (id == null) {
                                throw new DataAccessException("Failed to generate ID for entity: " + d.entity);
                            }
                            d.entity = updateEntityId(IdProperty, d.entity, id);
                        }
                        index++;
                    }
                }
            }
        };
    }

    abstract class MongoDbEntityOperation<T> extends AbstractSyncEntityOperations<MongoDbOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        /**
         * Create a new instance.
         *
         * @param ctx
         * @param persistentEntity The RuntimePersistentEntity
         * @param entity
         */
        protected MongoDbEntityOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(ctx,
                    DefaultMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultMongoDbRepositoryOperations.this.entityEventRegistry,
                    persistentEntity,
                    DefaultMongoDbRepositoryOperations.this.conversionService,
                    entity,
                    insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }
    }

    abstract class MongoDbEntitiesOperation<T> extends AbstractSyncEntitiesOperations<MongoDbOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        protected MongoDbEntitiesOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(ctx,
                    DefaultMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultMongoDbRepositoryOperations.this.conversionService,
                    DefaultMongoDbRepositoryOperations.this.entityEventRegistry,
                    persistentEntity,
                    entities,
                    insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }

    }

    protected static class MongoDbOperationContext extends OperationContext {

        private final ClientSession clientSession;

        public MongoDbOperationContext(ClientSession clientSession, Class<?> repositoryType, AnnotationMetadata annotationMetadata) {
            super(annotationMetadata, repositoryType);
            this.clientSession = clientSession;
        }
    }
}
