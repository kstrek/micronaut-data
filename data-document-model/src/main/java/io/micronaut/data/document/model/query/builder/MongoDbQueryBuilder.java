package io.micronaut.data.document.model.query.builder;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.annotation.TypeRole;
import io.micronaut.data.exceptions.MappingException;
import io.micronaut.data.model.Association;
import io.micronaut.data.model.Embedded;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.PersistentEntity;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.Sort;
import io.micronaut.data.model.naming.NamingStrategy;
import io.micronaut.data.model.query.BindingParameter;
import io.micronaut.data.model.query.JoinPath;
import io.micronaut.data.model.query.QueryModel;
import io.micronaut.data.model.query.builder.QueryBuilder;
import io.micronaut.data.model.query.builder.QueryParameterBinding;
import io.micronaut.data.model.query.builder.QueryResult;
import io.micronaut.serde.config.annotation.SerdeConfig;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MongoDbQueryBuilder implements QueryBuilder {

    protected final Map<Class, CriterionHandler> queryHandlers = new HashMap<>(30);

    {
        addCriterionHandler(QueryModel.Negation.class, (ctx, obj, negation) -> {
            if (negation.getCriteria().size() == 1) {
                QueryModel.Criterion criterion = negation.getCriteria().iterator().next();
                if (criterion instanceof QueryModel.In) {
                    QueryModel.In in = (QueryModel.In) criterion;
                    handleCriterion(ctx, obj, new QueryModel.NotIn(in.getName(), in.getValue()));
                    return;
                }
                if (criterion instanceof QueryModel.NotIn) {
                    QueryModel.NotIn notIn = (QueryModel.NotIn) criterion;
                    handleCriterion(ctx, obj, new QueryModel.In(notIn.getName(), notIn.getValue()));
                    return;
                }
                if (criterion instanceof QueryModel.PropertyCriterion || criterion instanceof QueryModel.PropertyComparisonCriterion) {
                    Map<String, Object> neg = new LinkedHashMap<>();
                    handleCriterion(ctx, neg, criterion);
                    if (neg.size() != 1) {
                        throw new IllegalStateException("Expected size of 1");
                    }
                    String key = neg.keySet().iterator().next();
                    obj.put(key, singletonMap("$neg", neg.get(key)));
                } else {
                    throw new IllegalStateException("Negation is not supported for this criterion: " + criterion);
                }
            } else {
                throw new IllegalStateException("Negation not supported on multiple criterion: " + negation);
            }
        });

        addCriterionHandler(QueryModel.Conjunction.class, (ctx, sb, conjunction) -> handleJunction(ctx, sb, conjunction, "$and"));
        addCriterionHandler(QueryModel.Disjunction.class, (ctx, sb, disjunction) -> handleJunction(ctx, sb, disjunction, "$or"));
        addCriterionHandler(QueryModel.IsTrue.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), true)));
        addCriterionHandler(QueryModel.IsFalse.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), false)));
        addCriterionHandler(QueryModel.Equals.class, propertyOperatorExpression("$eq"));
        addCriterionHandler(QueryModel.IdEquals.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals("id", criterion.getValue())));
        addCriterionHandler(QueryModel.VersionEquals.class, (context, sb, criterion) -> {
            handleCriterion(context, sb, new QueryModel.Equals(context.getPersistentEntity().getVersion().getName(), criterion.getValue()));
        });
        addCriterionHandler(QueryModel.NotEquals.class, propertyOperatorExpression("$ne"));
        addCriterionHandler(QueryModel.GreaterThan.class, propertyOperatorExpression("$gt"));
        addCriterionHandler(QueryModel.GreaterThanEquals.class, propertyOperatorExpression("$gte"));
        addCriterionHandler(QueryModel.LessThan.class, propertyOperatorExpression("$lt"));
        addCriterionHandler(QueryModel.LessThanEquals.class, propertyOperatorExpression("$lte"));
        addCriterionHandler(QueryModel.IsNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.IsNotNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.NotEquals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.IsNotNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.NotEquals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.GreaterThanProperty.class, comparison("$gt"));
        addCriterionHandler(QueryModel.GreaterThanEqualsProperty.class, comparison("$gte"));
        addCriterionHandler(QueryModel.LessThanProperty.class, comparison("$lt"));
        addCriterionHandler(QueryModel.LessThanEqualsProperty.class, comparison("$lte"));
        addCriterionHandler(QueryModel.EqualsProperty.class, comparison("$eq"));
        addCriterionHandler(QueryModel.NotEqualsProperty.class, comparison("$ne"));
        addCriterionHandler(QueryModel.Between.class, (context, obj, criterion) -> {
            QueryModel.Conjunction conjunction = new QueryModel.Conjunction();
            conjunction.add(new QueryModel.GreaterThanEquals(criterion.getProperty(), criterion.getFrom()));
            conjunction.add(new QueryModel.LessThanEquals(criterion.getProperty(), criterion.getTo()));
            handleCriterion(context, obj, conjunction);
        });
        addCriterionHandler(QueryModel.Regex.class, propertyOperatorExpression("$regex", value -> {
            if (value instanceof BindingParameter) {
                return value;
            }
            return new RegexPattern(value.toString());
        }));
        addCriterionHandler(QueryModel.IsEmpty.class, (context, obj, criterion) -> {
            obj.put("$or", asList(
                    singletonMap(criterion.getProperty(), singletonMap("$eq", "")),
                    singletonMap(criterion.getProperty(), singletonMap("$exists", false))
            ));
        });
        addCriterionHandler(QueryModel.IsNotEmpty.class, (context, obj, criterion) -> {
            obj.put("$and", asList(
                    singletonMap(criterion.getProperty(), singletonMap("$ne", "")),
                    singletonMap(criterion.getProperty(), singletonMap("$exists", true))
            ));
        });
        addCriterionHandler(QueryModel.In.class, (context, obj, criterion) -> {
            PersistentPropertyPath propertyPath = context.getRequiredProperty(criterion);
            Object value = criterion.getValue();
            if (value instanceof Iterable) {
                List<?> values = CollectionUtils.iterableToList((Iterable) value);
                obj.put(criterion.getProperty(), singletonMap("$in", values.stream().map(val -> valueRepresentation(context, propertyPath, val)).collect(Collectors.toList())));
            } else {
                obj.put(criterion.getProperty(), singletonMap("$in", singletonList(valueRepresentation(context, propertyPath, value))));
            }
        });
        addCriterionHandler(QueryModel.NotIn.class, (context, obj, criterion) -> {
            PersistentPropertyPath propertyPath = context.getRequiredProperty(criterion);
            Object value = criterion.getValue();
            if (value instanceof Iterable) {
                List<?> values = CollectionUtils.iterableToList((Iterable) value);
                obj.put(criterion.getProperty(), singletonMap("$nin", values.stream().map(val -> valueRepresentation(context, propertyPath, val)).collect(Collectors.toList())));
            } else {
                obj.put(criterion.getProperty(), singletonMap("$nin", singletonList(valueRepresentation(context, propertyPath, value))));
            }
        });
    }

    private <T extends QueryModel.PropertyCriterion> CriterionHandler<T> propertyOperatorExpression(String op) {
        return propertyOperatorExpression(op, null);
    }

    private <T extends QueryModel.PropertyCriterion> CriterionHandler<T> propertyOperatorExpression(String op, Function<Object, Object> mapper) {
        return (context, obj, criterion) -> {
            Object value = criterion.getValue();
            if (mapper != null) {
                value = mapper.apply(value);
            }
            PersistentPropertyPath propertyPath = context.getRequiredProperty(criterion);
            Object finalValue = value;
            traversePersistentProperties(propertyPath.getAssociations(), propertyPath.getProperty(), (associations, property) -> {
                String path = asPath(associations, property);
                obj.put(path, singletonMap(op, valueRepresentation(context, propertyPath, new PersistentPropertyPath(associations, property), finalValue)));
            });
        };
    }

    private String getPath(PersistentPropertyPath propertyPath) {
        PersistentProperty property = propertyPath.getProperty();
        String propertyName = getPropertyPersistName(property);
        StringJoiner sj = new StringJoiner(".");
        propertyPath.getAssociations().forEach(a -> sj.add(getPropertyPersistName(a)));
        sj.add(propertyName);
        return sj.toString();
    }

    private String getPropertyPersistName(PersistentProperty property) {
        if (property.getOwner().getIdentity() == property) {
            return "_id";
        }
        return property.getAnnotationMetadata()
                .stringValue(SerdeConfig.class, SerdeConfig.PROPERTY)
                .orElseGet(property::getName);
    }

    private Object valueRepresentation(CriteriaContext context, PersistentPropertyPath propertyPath, Object value) {
        return valueRepresentation(context, propertyPath, propertyPath, value);
    }

    private Object valueRepresentation(CriteriaContext context,
                                       PersistentPropertyPath inPropertyPath,
                                       PersistentPropertyPath outPropertyPath,
                                       Object value) {
        if (value instanceof BindingParameter) {
            int index = context.pushParameter(
                    (BindingParameter) value,
                    newBindingContext(inPropertyPath, outPropertyPath)
            );
            return singletonMap("$qpidx", index);
        } else {
            return asLiteral(value);
        }
    }

    private <T extends QueryModel.PropertyComparisonCriterion> CriterionHandler<T> comparison(String operator) {
        return (ctx, obj, comparisonCriterion) -> {
            PersistentPropertyPath p1 = ctx.getRequiredProperty(comparisonCriterion.getProperty(), comparisonCriterion.getClass());
            PersistentPropertyPath p2 = ctx.getRequiredProperty(comparisonCriterion.getOtherProperty(), comparisonCriterion.getClass());
            obj.put("$expr", singletonMap(
                    operator, asList(
                            "$" + p1.getPath(), "$" + p2.getPath()
                    )
            ));
        };
    }

    protected Object asLiteral(@Nullable Object value) {
        if (value instanceof RegexPattern) {
            return "'" + Pattern.quote(((RegexPattern) value).value) + "'";
        }
        return value;
    }

    @Override
    public QueryResult buildInsert(AnnotationMetadata repositoryMetadata, PersistentEntity entity) {
        return null;
    }

    @Override
    public QueryResult buildQuery(AnnotationMetadata annotationMetadata, QueryModel query) {
        ArgumentUtils.requireNonNull("annotationMetadata", annotationMetadata);
        ArgumentUtils.requireNonNull("query", query);

        QueryState queryState = new QueryState(query, true, false);

        Map<String, Object> predicateObj = new LinkedHashMap<>();
        Map<String, Object> projectionObj = new LinkedHashMap<>();
        Map<String, Object> countObj = new LinkedHashMap<>();
        Map<String, Object> sortObj = new LinkedHashMap<>();

        addLookups(query.getJoinPaths(), queryState);
        List<Map<String, Object>> pipeline = queryState.rootLookups.pipeline;
        buildProjection(query.getProjections(), query.getPersistentEntity(), projectionObj, countObj);
        QueryModel.Junction criteria = query.getCriteria();
        if (!criteria.isEmpty()) {
            predicateObj = buildWhereClause(annotationMetadata, criteria, queryState);
        }

        Sort sort = query.getSort();
        if (sort.isSorted()) {
            sort.getOrderBy().forEach(order -> sortObj.put(order.getProperty(), order.isAscending() ? 1 : -1));
        }
        if (!predicateObj.isEmpty()) {
            pipeline.add(singletonMap("$match", predicateObj));
        }
        if (!countObj.isEmpty()) {
            pipeline.add(countObj);
        }
        if (!projectionObj.isEmpty()) {
            pipeline.add(singletonMap("$project", projectionObj));
        }
        if (!sortObj.isEmpty()) {
            pipeline.add(singletonMap("$sort", sortObj));
        }
        if (query.getOffset() > 0) {
            pipeline.add(singletonMap("$skip", query.getOffset()));
        }
        if (query.getMax() != -1) {
            pipeline.add(singletonMap("$limit", query.getMax()));
        }

        String q;
        if (pipeline.isEmpty()) {
            q = "{}";
        } else if (isMatchOnlyStage(pipeline)) {
            q = toJsonString(predicateObj);
        } else {
            q = toJsonString(pipeline);
        }
        return new QueryResult() {

            @NonNull
            @Override
            public String getQuery() {
                return q;
            }

            @Override
            public int getMax() {
                return query.getMax();
            }

            @Override
            public long getOffset() {
                return query.getOffset();
            }

            @Override
            public List<String> getQueryParts() {
                return Collections.emptyList();
            }

            @Override
            public List<QueryParameterBinding> getParameterBindings() {
                return queryState.getParameterBindings();
            }

            @Override
            public Map<String, String> getAdditionalRequiredParameters() {
                return Collections.emptyMap();
            }

        };
    }

    private void addLookups(Collection<JoinPath> joins, QueryState queryState) {
        if (joins.isEmpty()) {
            return;
        }
        List<String> joined = joins.stream().map(JoinPath::getPath)
                .sorted((o1, o2) -> Comparator.comparingInt(String::length).thenComparing(String::compareTo).compare(o1, o2))
                .collect(Collectors.toList());
        for (String join : joined) {
            StringJoiner rootPath = new StringJoiner(".");
            StringJoiner currentEntityPath = new StringJoiner(".");
            LookupsStage currentLookup = queryState.rootLookups;
            for (String path : StringUtils.splitOmitEmptyStrings(join, '.')) {
                rootPath.add(path);
                currentEntityPath.add(path);
                String thisPath = currentEntityPath.toString();
                if (currentLookup.subLookups.containsKey(thisPath)) {
                    currentLookup = currentLookup.subLookups.get(path);
                    currentEntityPath = new StringJoiner(".");
                    continue;
                }

                PersistentPropertyPath propertyPath = currentLookup.persistentEntity.getPropertyPath(thisPath);
                PersistentProperty property = propertyPath.getProperty();
                if (!(property instanceof Association)) {
                    continue;
                }
                Association association = (Association) property;
                if (association.getKind() == Relation.Kind.EMBEDDED) {
                    continue;
                }
                LookupsStage lookupStage = new LookupsStage(association.getAssociatedEntity());
                List<Map<String, Object>> pipeline = currentLookup.pipeline;
                Optional<Association> inverseSide = association.getInverseSide().map(Function.identity());
                PersistentEntity persistentEntity = association.getOwner();

                String joinedCollectionName = association.getAssociatedEntity().getPersistedName();
                String ownerCollectionName = persistentEntity.getPersistedName();
                if (association.getKind() == Relation.Kind.MANY_TO_MANY || association.isForeignKey() && !inverseSide.isPresent()) {
                    PersistentEntity associatedEntity = association.getAssociatedEntity();
                    PersistentEntity associationOwner = association.getOwner();
                    // JOIN TABLE
                    PersistentProperty identity = associatedEntity.getIdentity();
                    if (identity == null) {
                        throw new IllegalArgumentException("Associated entity [" + associatedEntity.getName() + "] defines no ID. Cannot join.");
                    }
                    final PersistentProperty associatedId = associationOwner.getIdentity();
                    if (associatedId == null) {
                        throw new MappingException("Cannot join on entity [" + associationOwner.getName() + "] that has no declared ID");
                    }
                    Association owningAssociation = inverseSide.orElse(association);
                    boolean isAssociationOwner = !association.getInverseSide().isPresent();
                    NamingStrategy namingStrategy = associationOwner.getNamingStrategy();
                    AnnotationMetadata annotationMetadata = owningAssociation.getAnnotationMetadata();

                    List<String> ownerJoinFields = resolveJoinTableAssociatedFields(annotationMetadata, isAssociationOwner, associationOwner, namingStrategy);
                    List<String> ownerJoinCollectionFields = resolveJoinTableJoinFields(annotationMetadata, isAssociationOwner, associationOwner, namingStrategy);
                    List<String> associationJoinFields = resolveJoinTableAssociatedFields(annotationMetadata, !isAssociationOwner, associatedEntity, namingStrategy);
                    List<String> associationJoinCollectionFields = resolveJoinTableJoinFields(annotationMetadata, !isAssociationOwner, associatedEntity, namingStrategy);

                    String joinCollectionName = namingStrategy.mappedName(owningAssociation);

//                        String joinTableName = annotationMetadata
//                                .stringValue(ANN_JOIN_TABLE, "name")
//                                .orElseGet(() -> namingStrategy.mappedName(association));

                    List<Map<String, Object>> joinCollectionLookupPipeline = new ArrayList<>();
                    pipeline.add(lookup(joinCollectionName, "_id", ownerCollectionName, joinCollectionLookupPipeline, thisPath));
                    joinCollectionLookupPipeline.add(
                            lookup(
                                    joinedCollectionName,
                                    joinedCollectionName,
                                    "_id",
                                    lookupStage.pipeline,
                                    joinedCollectionName)
                    );
                    joinCollectionLookupPipeline.add(unwind("$" + joinedCollectionName, true));
                    joinCollectionLookupPipeline.add(
                            singletonMap("$replaceRoot", singletonMap("newRoot", "$" + joinedCollectionName))
                    );
                } else {
                    String currentPath = asPath(propertyPath.getAssociations(), propertyPath.getProperty());
                    if (association.isForeignKey()) {
                        String mappedBy = association.getAnnotationMetadata().stringValue(Relation.class, "mappedBy").get();
                        PersistentPropertyPath mappedByPath = association.getAssociatedEntity().getPropertyPath(mappedBy);
                        if (mappedByPath == null) {
                            throw new IllegalStateException("Cannot find mapped path: " + mappedBy);
                        }
                        if (!(mappedByPath.getProperty() instanceof Association)) {
                            throw new IllegalStateException("Expected association as a mapped path: " + mappedBy);
                        }

                        List<String> localMatchFields = new ArrayList<>();
                        List<String> foreignMatchFields = new ArrayList<>();
                        traversePersistentProperties(currentLookup.persistentEntity.getIdentity(), (associations, p) -> {
                            String fieldPath = asPath(associations, p);
                            localMatchFields.add(fieldPath);
                        });

                        List<Association> mappedAssociations = new ArrayList<>(mappedByPath.getAssociations());
                        mappedAssociations.add((Association) mappedByPath.getProperty());

                        traversePersistentProperties(mappedAssociations, currentLookup.persistentEntity.getIdentity(), (associations, p) -> {
                            String fieldPath = asPath(associations, p);
                            foreignMatchFields.add(fieldPath);
                        });

                        pipeline.add(lookup(
                                joinedCollectionName,
                                localMatchFields,
                                foreignMatchFields,
                                lookupStage.pipeline,
                                currentPath)
                        );
                    } else {
                        List<Association> mappedAssociations = new ArrayList<>(propertyPath.getAssociations());
                        mappedAssociations.add((Association) propertyPath.getProperty());

                        List<String> localMatchFields = new ArrayList<>();
                        List<String> foreignMatchFields = new ArrayList<>();
                        traversePersistentProperties(mappedAssociations, lookupStage.persistentEntity.getIdentity(), (associations, p) -> {
                            String fieldPath = asPath(associations, p);
                            localMatchFields.add(fieldPath);
                        });
                        traversePersistentProperties(lookupStage.persistentEntity.getIdentity(), (associations, p) -> {
                            String fieldPath = asPath(associations, p);
                            foreignMatchFields.add(fieldPath);
                        });

                        pipeline.add(lookup(
                                joinedCollectionName,
                                localMatchFields,
                                foreignMatchFields,
                                lookupStage.pipeline,
                                currentPath)
                        );
                    }
                    if (association.getKind().isSingleEnded()) {
                        pipeline.add(unwind("$" + currentPath, true));
                    }
                }
                currentLookup.subLookups.put(currentEntityPath.toString(), lookupStage);
            }
            queryState.joinPaths.add(join);
        }
    }

    @NonNull
    private List<String> resolveJoinTableJoinFields(AnnotationMetadata annotationMetadata, boolean associationOwner, PersistentEntity entity, NamingStrategy namingStrategy) {
        List<String> joinColumns = getJoinedFields(annotationMetadata, associationOwner, "name");
        if (!joinColumns.isEmpty()) {
            return joinColumns;
        }
        List<String> fields = new ArrayList<>();
        traversePersistentProperties(entity.getIdentity(), (associations, property) -> {
            fields.add(asPath(associations, property));
        });
        return fields;
    }

    @NonNull
    private List<String> resolveJoinTableAssociatedFields(AnnotationMetadata annotationMetadata, boolean associationOwner, PersistentEntity entity, NamingStrategy namingStrategy) {
        List<String> joinColumns = getJoinedFields(annotationMetadata, associationOwner, "referencedColumnName");
        if (!joinColumns.isEmpty()) {
            return joinColumns;
        }
        PersistentProperty identity = entity.getIdentity();
        if (identity == null) {
            throw new MappingException("Cannot have a foreign key association without an ID on entity: " + entity.getName());
        }
        List<String> fields = new ArrayList<>();
        traversePersistentProperties(identity, (associations, property) -> {
            fields.add(asPath(associations, property));
        });
        return fields;
    }

    @NonNull
    private List<String> getJoinedFields(AnnotationMetadata annotationMetadata, boolean associationOwner, String columnType) {
//        AnnotationValue<Annotation> joinTable = annotationMetadata.getAnnotation(ANN_JOIN_TABLE);
//        if (joinTable != null) {
//            return joinTable.getAnnotations(associationOwner ? "joinColumns" : "inverseJoinColumns")
//                    .stream()
//                    .map(ann -> ann.stringValue(columnType).orElse(null))
//                    .filter(Objects::nonNull)
//                    .collect(Collectors.toList());
//        }
        return Collections.emptyList();
    }

    private String asPath(List<Association> associations, PersistentProperty property) {
        if (associations.isEmpty()) {
            return getPropertyPersistName(property);
        }
        StringJoiner joiner = new StringJoiner(".");
        for (Association association : associations) {
            joiner.add(getPropertyPersistName(association));
        }
        joiner.add(getPropertyPersistName(property));
        return joiner.toString();
    }

    private static class LookupsStage {

        final PersistentEntity persistentEntity;

        List<Map<String, Object>> pipeline = new ArrayList<>();
        Map<String, LookupsStage> subLookups = new HashMap<>();

        private LookupsStage(PersistentEntity persistentEntity) {
            this.persistentEntity = persistentEntity;
        }
    }

    private Map<String, Object> lookup(String from, String localField, String foreignField, List<Map<String, Object>> pipeline, String as) {
        Map<String, Object> lookup = new LinkedHashMap<>();
        lookup.put("from", from);
        lookup.put("localField", localField);
        lookup.put("foreignField", foreignField);
        lookup.put("pipeline", pipeline);
        lookup.put("as", as);
        return singletonMap("$lookup", lookup);
    }

    private Map<String, Object> lookup(String from,
                                       List<String> localFields,
                                       List<String> foreignFields,
                                       List<Map<String, Object>> pipeline,
                                       String as) {
        if (localFields.size() != foreignFields.size()) {
            throw new IllegalStateException("Un-matching join columns size: " + localFields.size() + " != " + foreignFields.size() + " " + localFields + ", " + foreignFields);
        }
        if (localFields.size() == 1) {
            return lookup(from, localFields.iterator().next(), foreignFields.iterator().next(), pipeline, as);
        }
        List<Map<String, Object>> matches = new ArrayList<>(localFields.size());
        Map<String, Object> let = new LinkedHashMap<>();
        int i = 1;
        Iterator<String> foreignIt = foreignFields.iterator();
        for (String localField : localFields) {
            String var = "v" + i++;
            let.put(var, "$" + localField);
            matches.add(singletonMap("$eq", Arrays.asList("$$" + var, "$" + foreignIt.next())));
        }

        Map<String, Object> match;
        if (matches.size() > 1) {
            match = singletonMap("$match", singletonMap("$expr", singletonMap("$and", matches)));
        } else {
            match = singletonMap("$match", singletonMap("$expr", matches.iterator().next()));
        }

        return lookup(from, let, match, pipeline, as);
    }

    private Map<String, Object> lookup(String from,
                                       Map<String, Object> let,
                                       Map<String, Object> match,
                                       List<Map<String, Object>> pipeline,
                                       String as) {

        pipeline.add(match);
        Map<String, Object> lookup = new LinkedHashMap<>();
        lookup.put("from", from);
        lookup.put("let", let);
        lookup.put("pipeline", pipeline);
        lookup.put("as", as);
        return singletonMap("$lookup", lookup);
    }

    private Map<String, Object> unwind(String path, boolean preserveNullAndEmptyArrays) {
        Map<String, Object> unwind = new LinkedHashMap<>();
        unwind.put("path", path);
        unwind.put("preserveNullAndEmptyArrays", preserveNullAndEmptyArrays);
        return singletonMap("$unwind", unwind);
    }

    private boolean isMatchOnlyStage(List<Map<String, Object>> pipeline) {
        return pipeline.size() == 1 && pipeline.iterator().next().containsKey("$match");
    }

    private Map<String, Object> buildWhereClause(AnnotationMetadata annotationMetadata, QueryModel.Junction criteria, QueryState queryState) {
        CriteriaContext ctx = new CriteriaContext() {

            @Override
            public QueryState getQueryState() {
                return queryState;
            }

            @Override
            public PersistentEntity getPersistentEntity() {
                return queryState.getEntity();
            }

            @Override
            public PersistentPropertyPath getRequiredProperty(String name, Class<?> criterionClazz) {
                return findProperty(queryState, name, criterionClazz);
            }

        };

        Map<String, Object> obj = new LinkedHashMap<>();
        handleCriterion(ctx, obj, criteria);
        return obj;
    }

    private void buildProjection(List<QueryModel.Projection> projectionList,
                                 PersistentEntity entity,
                                 Map<String, Object> projectionObj,
                                 Map<String, Object> countObj) {
        if (projectionList.isEmpty()) {
        } else {
            for (Iterator i = projectionList.iterator(); i.hasNext(); ) {
                QueryModel.Projection projection = (QueryModel.Projection) i.next();
                if (projection instanceof QueryModel.LiteralProjection) {

                } else if (projection instanceof QueryModel.CountProjection) {
                    countObj.put("$count", "result");
                } else if (projection instanceof QueryModel.DistinctProjection) {
                } else if (projection instanceof QueryModel.IdProjection) {
                    projectionObj.put("_id", 1);
                } else if (projection instanceof QueryModel.PropertyProjection) {
                    QueryModel.PropertyProjection pp = (QueryModel.PropertyProjection) projection;
                    if (projection instanceof QueryModel.AvgProjection) {
                    } else if (projection instanceof QueryModel.DistinctPropertyProjection) {
                        addProjection(projectionObj, pp, "$max");
                    } else if (projection instanceof QueryModel.SumProjection) {
                        addProjection(projectionObj, pp, "$sum");
                    } else if (projection instanceof QueryModel.MinProjection) {
                        addProjection(projectionObj, pp, "$min");
                    } else if (projection instanceof QueryModel.MaxProjection) {
                        addProjection(projectionObj, pp, "$max");
                    } else if (projection instanceof QueryModel.CountDistinctProjection) {
                    } else {
                        String propertyName = pp.getPropertyName();
                        PersistentPropertyPath propertyPath = entity.getPropertyPath(propertyName);
                        if (propertyPath == null) {
                            throw new IllegalArgumentException("Cannot project on non-existent property: " + propertyName);
                        }
                        projectionObj.put(getPropertyPersistName(propertyPath.getProperty()), 1);
                    }
                }
            }
        }
    }

    private void addProjection(Map<String, Object> groupBy, QueryModel.PropertyProjection pr, String op) {
        groupBy.put(pr.getPropertyName(), singletonMap(op, "$" + pr.getPropertyName()));
    }

    @NonNull
    private PersistentPropertyPath findProperty(QueryState queryState, String name, Class criterionType) {
        return findPropertyInternal(queryState, queryState.getEntity(), queryState.getRootAlias(), name, criterionType);
    }

    private PersistentPropertyPath findPropertyInternal(QueryState queryState, PersistentEntity entity, String tableAlias, String name, Class criterionType) {
        PersistentPropertyPath propertyPath = entity.getPropertyPath(name);
        if (propertyPath != null) {
            if (propertyPath.getAssociations().isEmpty()) {
                return propertyPath;
            }
            Association joinAssociation = null;
            StringJoiner joinPathJoiner = new StringJoiner(".");
            for (Association association : propertyPath.getAssociations()) {
                joinPathJoiner.add(association.getName());
                if (association instanceof Embedded) {
                    continue;
                }
                if (joinAssociation == null) {
                    joinAssociation = association;
                    continue;
                }
                if (association != joinAssociation.getAssociatedEntity().getIdentity()) {
                    if (!queryState.isAllowJoins()) {
                        throw new IllegalArgumentException("Joins cannot be used in a DELETE or UPDATE operation");
                    }
                    String joinStringPath = joinPathJoiner.toString();
                    if (!queryState.isJoined(joinStringPath)) {
                        throw new IllegalArgumentException("Property is not joined at path: " + joinStringPath);
                    }
//                    lastJoinAlias = joinInPath(queryState, joinStringPath);
                    // Continue to look for a joined property
                    joinAssociation = association;
                } else {
                    // We don't need to join to access the id of the relation
                    joinAssociation = null;
                }
            }
            PersistentProperty property = propertyPath.getProperty();
            if (joinAssociation != null) {
                if (property != joinAssociation.getAssociatedEntity().getIdentity()) {
                    String joinStringPath = joinPathJoiner.toString();
                    if (!queryState.isJoined(joinStringPath)) {
                        throw new IllegalArgumentException("Property is not joined at path: " + joinStringPath);
                    }
                }
                // We don't need to join to access the id of the relation
            }
        } else if (TypeRole.ID.equals(name) && entity.getIdentity() != null) {
            // special case handling for ID
            return new PersistentPropertyPath(Collections.emptyList(), entity.getIdentity(), entity.getIdentity().getName());
        }
        if (propertyPath == null) {
            if (criterionType == null || criterionType == Sort.Order.class) {
                throw new IllegalArgumentException("Cannot order on non-existent property path: " + name);
            } else {
                throw new IllegalArgumentException("Cannot use [" + criterionType.getSimpleName() + "] criterion on non-existent property path: " + name);
            }
        }
        return propertyPath;
    }

    private void handleJunction(CriteriaContext ctx, Map<String, Object> query, QueryModel.Junction criteria, String operator) {
        if (criteria.getCriteria().size() == 1) {
            handleCriterion(ctx, query, criteria.getCriteria().iterator().next());
        } else {
            List<Object> ops = new ArrayList<>(criteria.getCriteria().size());
            query.put(operator, ops);
            for (QueryModel.Criterion criterion : criteria.getCriteria()) {
                Map<String, Object> criterionObj = new LinkedHashMap<>();
                ops.add(criterionObj);
                handleCriterion(ctx, criterionObj, criterion);
            }
        }
    }

    private void handleCriterion(CriteriaContext ctx, Map<String, Object> query, QueryModel.Criterion criterion) {
        CriterionHandler<QueryModel.Criterion> criterionHandler = queryHandlers.get(criterion.getClass());
        if (criterionHandler == null) {
            throw new IllegalArgumentException("Queries of type " + criterion.getClass().getSimpleName() + " are not supported by this implementation");
        }
        criterionHandler.handle(ctx, query, criterion);
    }

    @Override
    public QueryResult buildUpdate(AnnotationMetadata annotationMetadata, QueryModel query, List<String> propertiesToUpdate) {
        throw new IllegalStateException("Not supported!");
    }

    @Override
    public QueryResult buildUpdate(AnnotationMetadata annotationMetadata, QueryModel query, Map<String, Object> propertiesToUpdate) {
        ArgumentUtils.requireNonNull("annotationMetadata", annotationMetadata);
        ArgumentUtils.requireNonNull("query", query);
        ArgumentUtils.requireNonNull("propertiesToUpdate", propertiesToUpdate);

        QueryState queryState = new QueryState(query, true, false);

        QueryModel.Junction criteria = query.getCriteria();

        String predicateQuery = "";
        if (!criteria.isEmpty()) {
            Map<String, Object> predicate = buildWhereClause(annotationMetadata, criteria, queryState);
            predicateQuery = toJsonString(predicate);
        }

        Map<String, Object> sets = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : propertiesToUpdate.entrySet()) {
            if (e.getValue() instanceof BindingParameter) {
                PersistentPropertyPath propertyPath = findProperty(queryState, e.getKey(), null);
                int index = queryState.pushParameter(
                        (BindingParameter) e.getValue(),
                        newBindingContext(propertyPath)
                );
                sets.put(e.getKey(), singletonMap("$qpidx", index));
            } else {
                sets.put(e.getKey(), e.getValue());
            }
        }

        String update = toJsonString(singletonMap("$set", sets));

        String finalPredicateQuery = predicateQuery;
        return new QueryResult() {

            @NonNull
            @Override
            public String getQuery() {
                return finalPredicateQuery;
            }

            @Override
            public String getUpdate() {
                return update;
            }

            @Override
            public List<String> getQueryParts() {
                return Collections.emptyList();
            }

            @Override
            public List<QueryParameterBinding> getParameterBindings() {
                return queryState.getParameterBindings();
            }

            @Override
            public Map<String, String> getAdditionalRequiredParameters() {
                return Collections.emptyMap();
            }

        };
    }

    @Override
    public QueryResult buildDelete(AnnotationMetadata annotationMetadata, QueryModel query) {
        ArgumentUtils.requireNonNull("annotationMetadata", annotationMetadata);
        ArgumentUtils.requireNonNull("query", query);

        QueryState queryState = new QueryState(query, true, false);

        QueryModel.Junction criteria = query.getCriteria();

        String predicateQuery = "";
        if (!criteria.isEmpty()) {
            Map<String, Object> predicate = buildWhereClause(annotationMetadata, criteria, queryState);
            predicateQuery = toJsonString(predicate);
        }

        return QueryResult.of(
                predicateQuery,
                Collections.emptyList(),
                queryState.getParameterBindings(),
                queryState.getAdditionalRequiredParameters(),
                query.getMax(),
                query.getOffset()
        );
    }

    @Override
    public QueryResult buildOrderBy(PersistentEntity entity, Sort sort) {
        return null;
    }

    @Override
    public QueryResult buildPagination(Pageable pageable) {
        return null;
    }

    private String toJsonString(Object obj) {
        StringBuilder sb = new StringBuilder();
        append(sb, obj);
        return sb.toString();
    }

    private void appendMap(StringBuilder sb, Map<String, Object> map) {
        sb.append("{");
        for (Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> e = iterator.next();
            String key = e.getKey();
            if (shouldEscapeKey(key)) {
                sb.append("'").append(key).append("'");
            } else {
                sb.append(key);
            }
            sb.append(":");
            append(sb, e.getValue());
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("}");
    }

    private void appendArray(StringBuilder sb, Collection<Object> collection) {
        sb.append("[");
        for (Iterator<Object> iterator = collection.iterator(); iterator.hasNext(); ) {
            Object value = iterator.next();
            append(sb, value);
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");
    }

    private void append(StringBuilder sb, Object obj) {
        if (obj instanceof Map) {
            appendMap(sb, (Map) obj);
        } else if (obj instanceof Collection) {
            appendArray(sb, (Collection<Object>) obj);
        } else if (obj == null) {
            sb.append("null");
        } else if (obj instanceof Boolean) {
            sb.append(obj.toString().toLowerCase(Locale.ROOT));
        } else if (obj instanceof Number) {
            sb.append(obj);
        } else {
            sb.append("\'");
            sb.append(obj);
            sb.append("\'");
        }
    }

    private boolean shouldEscapeKey(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isAlphabetic(c) && !Character.isDigit(c) && c != '$') {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds criterion handler.
     *
     * @param clazz   The handler class
     * @param handler The handler
     * @param <T>     The criterion type
     */
    protected <T extends QueryModel.Criterion> void addCriterionHandler(Class<T> clazz, CriterionHandler<T> handler) {
        queryHandlers.put(clazz, handler);
    }

    private BindingParameter.BindingContext newBindingContext(@Nullable PersistentPropertyPath ref) {
        return newBindingContext(ref, ref);
    }

    private BindingParameter.BindingContext newBindingContext(@Nullable PersistentPropertyPath in, @Nullable PersistentPropertyPath out) {
        return BindingParameter.BindingContext.create()
                .incomingMethodParameterProperty(in)
                .outgoingQueryParameterProperty(out);
    }

    /**
     * Traverses properties that should be persisted.
     *
     * @param property The property to start traversing from
     * @param consumer The function to invoke on every property
     */
    private void traversePersistentProperties(PersistentProperty property, BiConsumer<List<Association>, PersistentProperty> consumer) {
        traversePersistentProperties(Collections.emptyList(), property, consumer);
    }

    private void traversePersistentProperties(List<Association> associations,
                                              PersistentProperty property,
                                              BiConsumer<List<Association>, PersistentProperty> consumerProperty) {
        if (property instanceof Embedded) {
            Embedded embedded = (Embedded) property;
            PersistentEntity embeddedEntity = embedded.getAssociatedEntity();
            Collection<? extends PersistentProperty> embeddedProperties = embeddedEntity.getPersistentProperties();
            List<Association> newAssociations = new ArrayList<>(associations);
            newAssociations.add((Association) property);
            for (PersistentProperty embeddedProperty : embeddedProperties) {
                traversePersistentProperties(newAssociations, embeddedProperty, consumerProperty);
            }
        } else if (property instanceof Association) {
            Association association = (Association) property;
            if (association.isForeignKey()) {
                return;
            }
            List<Association> newAssociations = new ArrayList<>(associations);
            newAssociations.add((Association) property);
            PersistentEntity associatedEntity = association.getAssociatedEntity();
            PersistentProperty assocIdentity = associatedEntity.getIdentity();
            if (assocIdentity == null) {
                throw new IllegalStateException("Identity cannot be missing for: " + associatedEntity);
            }
            if (assocIdentity instanceof Association) {
                traversePersistentProperties(newAssociations, assocIdentity, consumerProperty);
            } else {
                consumerProperty.accept(newAssociations, assocIdentity);
            }
        } else {
            consumerProperty.accept(associations, property);
        }
    }

    /**
     * A criterion handler.
     *
     * @param <T> The criterion type
     */
    protected interface CriterionHandler<T extends QueryModel.Criterion> {

        /**
         * Handles a criterion.
         *
         * @param context   The context
         * @param criterion The criterion
         */
        void handle(CriteriaContext context, Map<String, Object> query, T criterion);
    }

    /**
     * A criterion context.
     */
    protected interface CriteriaContext extends PropertyParameterCreator {

        QueryState getQueryState();

        PersistentEntity getPersistentEntity();

        PersistentPropertyPath getRequiredProperty(String name, Class<?> criterionClazz);

        default int pushParameter(@NotNull BindingParameter bindingParameter, @NotNull BindingParameter.BindingContext bindingContext) {
            return getQueryState().pushParameter(bindingParameter, bindingContext);
        }

        default PersistentPropertyPath getRequiredProperty(QueryModel.PropertyNameCriterion propertyCriterion) {
            return getRequiredProperty(propertyCriterion.getProperty(), propertyCriterion.getClass());
        }

    }

    /**
     * The state of the query.
     */
    @Internal
    protected final class QueryState implements PropertyParameterCreator {
        private final String rootAlias;
        private final Set<String> joinPaths = new TreeSet<>();
        private final AtomicInteger position = new AtomicInteger(0);
        private final Map<String, String> additionalRequiredParameters = new LinkedHashMap<>();
        private final List<QueryParameterBinding> parameterBindings;
        private final boolean allowJoins;
        private final QueryModel queryObject;
        private final boolean escape;
        private final PersistentEntity entity;

        private final LookupsStage rootLookups;

        private QueryState(QueryModel query, boolean allowJoins, boolean useAlias) {
            this.allowJoins = allowJoins;
            this.queryObject = query;
            this.entity = query.getPersistentEntity();
            this.escape = shouldEscape();
            this.rootAlias = useAlias ? null : null;
            this.parameterBindings = new ArrayList<>(entity.getPersistentPropertyNames().size());
            this.rootLookups = new LookupsStage(entity);
        }

        /**
         * @return The root alias
         */
        public @Nullable
        String getRootAlias() {
            return rootAlias;
        }

        /**
         * @return The entity
         */
        public PersistentEntity getEntity() {
            return entity;
        }

//        /**
//         * Add a required parameter.
//         *
//         * @param name The name
//         * @return name A placeholder in a query
//         */
//        public String addAdditionalRequiredParameter(@NonNull String name) {
//            AbstractSqlLikeQueryBuilder.Placeholder placeholder = newParameter();
//            additionalRequiredParameters.put(placeholder.key, name);
//            return placeholder.name;
//        }

        /**
         * @return The query string
         */

        /**
         * @return Does the query allow joins
         */
        public boolean isAllowJoins() {
            return allowJoins;
        }

//        /**
//         * Applies a join for the given association.
//         *
//         * @param jp The join path
//         * @return The alias
//         */
//        public String applyJoin(@NonNull JoinPath jp) {
//            String joinAlias = appliedJoinPaths.get(jp.getPath());
//            if (joinAlias != null) {
//                return joinAlias;
//            }
//            Optional<JoinPath> ojp = getQueryModel().getJoinPath(jp.getPath());
//            if (ojp.isPresent()) {
//                jp = ojp.get();
//            }
//            StringBuilder stringBuilder = getQuery();
//            Join.Type jt = jp.getJoinType();
//            String joinType = resolveJoinType(jt);
//
//            String[] associationAlias = buildJoin(
//                    getRootAlias(),
//                    jp,
//                    joinType,
//                    stringBuilder,
//                    appliedJoinPaths,
//                    this);
//            Association[] associationArray = jp.getAssociationPath();
//            StringJoiner associationPath = new StringJoiner(".");
//            String lastAlias = null;
//            for (int i = 0; i < associationAlias.length; i++) {
//                associationPath.add(associationArray[i].getName());
//                String computedAlias = associationAlias[i];
//                appliedJoinPaths.put(associationPath.toString(), computedAlias);
//                lastAlias = computedAlias;
//            }
//            return lastAlias;
//        }
//

        /**
         * Checks if the path is joined already.
         *
         * @param associationPath The association path.
         * @return true if joined
         */
        public boolean isJoined(String associationPath) {
            for (String joinPath : joinPaths) {
                if (joinPath.startsWith(associationPath)) {
                    return true;
                }
            }
            return joinPaths.contains(associationPath);
        }

        /**
         * @return Should escape the query
         */
        public boolean shouldEscape() {
            return escape;
        }

        /**
         * The additional required parameters.
         *
         * @return The parameters
         */
        public @NotNull Map<String, String> getAdditionalRequiredParameters() {
            return this.additionalRequiredParameters;
        }

        /**
         * The parameter binding.
         *
         * @return The parameter binding
         */
        public List<QueryParameterBinding> getParameterBindings() {
            return parameterBindings;
        }

        @Override
        public int pushParameter(@NotNull BindingParameter bindingParameter, @NotNull BindingParameter.BindingContext bindingContext) {
            int index = position.getAndIncrement();
            bindingContext = bindingContext.index(index);
            parameterBindings.add(
                    bindingParameter.bind(bindingContext)
            );
            return index;
        }
    }

    private interface PropertyParameterCreator {

        int pushParameter(@NotNull BindingParameter bindingParameter,
                          @NotNull BindingParameter.BindingContext bindingContext);

    }

    /**
     * Represents a placeholder in query.
     */
    public static final class Placeholder {
        private final String name;
        private final String key;

        /**
         * Default constructor.
         *
         * @param name The name of the place holder
         * @param key  The key to set the value of the place holder
         */
        public Placeholder(String name, String key) {
            this.name = name;
            this.key = key;
        }

        @Override
        public String toString() {
            return name;
        }

        /**
         * @return The place holder name
         */
        public String getName() {
            return name;
        }

        /**
         * This the precomputed key to set the place holder. In SQL this would be the index.
         *
         * @return The key used to set the placeholder.
         */
        public String getKey() {
            return key;
        }
    }

    /**
     * Represents a path to a property.
     */
    protected class QueryPropertyPath {
        private final PersistentPropertyPath propertyPath;
        private final String tableAlias;

        /**
         * Default constructor.
         *
         * @param propertyPath The propertyPath
         * @param tableAlias   The tableAlias
         */
        public QueryPropertyPath(@NotNull PersistentPropertyPath propertyPath, @Nullable String tableAlias) {
            this.propertyPath = propertyPath;
            this.tableAlias = tableAlias;
        }

        /**
         * @return The associations
         */
        @NonNull
        public List<Association> getAssociations() {
            return propertyPath.getAssociations();
        }

        /**
         * @return The property
         */
        @NonNull
        public PersistentProperty getProperty() {
            return propertyPath.getProperty();
        }

        /**
         * @return The path
         */
        @NonNull
        public String getPath() {
            return propertyPath.getPath();
        }

        /**
         * @return The path
         */
        @Nullable
        public String getTableAlias() {
            return tableAlias;
        }

//        /**
//         * @return already escaped column name
//         */
//        public String getColumnName() {
//            String columnName = getNamingStrategy().mappedName(propertyPath.getAssociations(), propertyPath.getProperty());
//            if (shouldEscape()) {
//                return quote(columnName);
//            }
//            return columnName;
//        }

        /**
         * @return the naming strategy
         */
        public NamingStrategy getNamingStrategy() {
            return propertyPath.getNamingStrategy();
        }

    }

    private static class RegexPattern {
        private final String value;

        private RegexPattern(String value) {
            this.value = value;
        }
    }
}
