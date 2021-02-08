package com.knoldus.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class UserSerializer implements Serializer {
    @Override public void configure(Map configs, boolean isKey) {

    }

    @Override public byte[] serialize(String s, Object o) {
        byte[] user = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            user = objectMapper.writeValueAsString(o).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return user;
    }



    @Override public void close() {

    }
}
