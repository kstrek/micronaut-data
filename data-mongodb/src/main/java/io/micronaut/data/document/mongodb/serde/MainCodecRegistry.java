package io.micronaut.data.document.mongodb.serde;

import jakarta.inject.Singleton;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

@Singleton
public class MainCodecRegistry {

    private final List<CodecRegistry> codecRegistries;

    public MainCodecRegistry(List<CodecRegistry> codecRegistries) {
        this.codecRegistries = codecRegistries;
    }

    public List<CodecRegistry> getCodecRegistries() {
        return codecRegistries;
    }
}
