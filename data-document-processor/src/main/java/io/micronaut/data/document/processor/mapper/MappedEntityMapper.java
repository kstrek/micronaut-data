package io.micronaut.data.document.processor.mapper;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.inject.annotation.TypedAnnotationMapper;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.serde.annotation.Serdeable;

import java.util.Arrays;
import java.util.List;

public class MappedEntityMapper implements TypedAnnotationMapper<MappedEntity> {

    @Override
    public Class<MappedEntity> annotationType() {
        return MappedEntity.class;
    }

    @Override
    public List<AnnotationValue<?>> map(AnnotationValue<MappedEntity> annotation, VisitorContext visitorContext) {
        return Arrays.asList(
                AnnotationValue.builder(Serdeable.Serializable.class).member("enabled", true).build(),
                AnnotationValue.builder(Serdeable.Deserializable.class).member("enabled", true).build()
        );
    }

}
