package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import jakarta.inject.Singleton;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DataCodecRegistry implements CodecRegistry {

    private final DataSerdeRegistry dataSerdeRegistry;
    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final Map<Class, Codec> codecs = new ConcurrentHashMap<>();

    public DataCodecRegistry(DataSerdeRegistry dataSerdeRegistry,
                             RuntimeEntityRegistry runtimeEntityRegistry) {
        this.dataSerdeRegistry = dataSerdeRegistry;
        this.runtimeEntityRegistry = runtimeEntityRegistry;
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz) {
        throw new CodecConfigurationException("Not supported");
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        Codec codec = codecs.get(clazz);
        if (codec != null) {
            return codec;
        }
        if (BeanIntrospector.SHARED.findIntrospection(clazz).isPresent()) {
            codec = new DataCodec<>(dataSerdeRegistry, runtimeEntityRegistry.getEntity(clazz), clazz, registry);
            codecs.put(clazz, codec);
            return codec;
        }
        return null;
    }

}
