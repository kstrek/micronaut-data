package io.micronaut.data.document.processor.mapper;

import io.micronaut.core.annotation.AnnotationClassValue;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.AnnotationValueBuilder;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.data.document.serde.CustomConverterDeserializer;
import io.micronaut.data.document.serde.CustomConverterSerializer;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.inject.annotation.TypedAnnotationMapper;
import io.micronaut.inject.ast.ClassElement;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.serde.config.annotation.SerdeConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MappedPropertyMapper implements TypedAnnotationMapper<io.micronaut.data.annotation.MappedProperty> {

    @Override
    public Class<io.micronaut.data.annotation.MappedProperty> annotationType() {
        return io.micronaut.data.annotation.MappedProperty.class;
    }

    @Override
    public List<AnnotationValue<?>> map(AnnotationValue<MappedProperty> annotation, VisitorContext visitorContext) {
        AnnotationValueBuilder<SerdeConfig> builder = AnnotationValue.builder(SerdeConfig.class);
        annotation.stringValue().ifPresent(property -> {
            builder.member(SerdeConfig.PROPERTY, property);
        });
        annotation.stringValue("converter").ifPresent(val -> {
            visitorContext.getClassElement(val).ifPresent(attributeConverterClassElement -> {
                ClassElement genericType = attributeConverterClassElement.getGenericType();
                Map<String, ClassElement> typeArguments = genericType.getTypeArguments(AttributeConverter.class.getName());
                if (typeArguments.isEmpty()) {
                    typeArguments = genericType.getTypeArguments("javax.persistence.AttributeConverter");
                }
                if (typeArguments.isEmpty()) {
                    typeArguments = genericType.getTypeArguments("jakarta.persistence.AttributeConverter");
                }
                ClassElement converterPersistedType = typeArguments.get("Y");
                if (converterPersistedType != null) {
                    builder.member(SerdeConfig.SERIALIZE_AS, new AnnotationClassValue<Object>(converterPersistedType.getName()));
                    builder.member(SerdeConfig.DESERIALIZE_AS, new AnnotationClassValue<Object>(converterPersistedType.getName()));
                }
            });
            builder.member(SerdeConfig.SERIALIZER_CLASS, CustomConverterSerializer.class);
            builder.member(SerdeConfig.DESERIALIZER_CLASS, CustomConverterDeserializer.class);
        });
        return Collections.singletonList(builder.build());
    }

}
