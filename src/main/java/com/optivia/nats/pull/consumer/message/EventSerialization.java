package com.optivia.nats.pull.consumer.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public interface EventSerialization {
    void serialize(ByteArrayOutputStream output) throws IOException;
    EventMessage deserialize(ByteArrayInputStream input) throws IOException;
}
