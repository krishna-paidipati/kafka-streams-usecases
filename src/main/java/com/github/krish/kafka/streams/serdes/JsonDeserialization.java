package com.github.krish.kafka.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;

class JsonDeserialization<T> implements Deserializer<T> {

    private Class<T> deserializeClass;

    public JsonDeserialization(Class<T> deserializeClass) {
        this.deserializeClass = deserializeClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return JsonSerialization.MAPPER.readValue(data, deserializeClass);
        } catch (Exception e) {
            //ingore
        }
        return null;
    }
}
