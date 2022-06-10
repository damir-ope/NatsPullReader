package com.optivia.nats.pull.consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NatsReaderConfigurationTest {
    private static final Integer connectByteBufferSize = 20*1024*1024;
    // 1 second
    private static final Integer initialMaxWaitTimeMs = 1000;
    private static final Integer maxWaitTimeMs = 100;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }
//    @Test
//    public void testNullSubjectNameConfig() throws IOException, InterruptedException {
//        Assertions.assertThrows(NullPointerException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, null,
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", "durName",
//                    1000, 3,
//                    40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testEmptySubjectNameConfig() throws IOException, InterruptedException {
//        Assertions.assertThrows(IllegalArgumentException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", "durName",
//                    1000, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testNullEndpointConfig() throws IOException, InterruptedException {
//        Assertions.assertThrows(NullPointerException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    null, "queueName",
//                    "events", "durName",
//                    1000, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testEmptyEndpointConfig() throws IOException, InterruptedException {
//        Assertions.assertThrows(IllegalArgumentException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "", "queueName",
//                    "events", "durName",
//                    1000, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testNullDurableName() throws IOException, InterruptedException {
//        Assertions.assertThrows(NullPointerException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", null,
//                    1000, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testEmptyDurableName() throws IOException, InterruptedException {
//        Assertions.assertThrows(IllegalArgumentException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", "",
//                    1000, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testZeroBatchSize() throws IOException, InterruptedException {
//        Assertions.assertThrows(IllegalArgumentException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", "durName",
//                    -1, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    public void testLesThanZeroBatchSize() throws IOException, InterruptedException {
//        Assertions.assertThrows(IllegalArgumentException.class, () -> {
//            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                    "Events.uuid.timeId.*.*",
//                    "PULL", false ,
//                    "nats://localhost:4222", "queueName",
//                    "events", "durName",
//                    0, 3,  40, connectByteBufferSize,
//                    initialMaxWaitTimeMs, maxWaitTimeMs);            });
//    }
//    @Test
//    void testValidConfiguration() {
//        NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
//                                                                "Events.uuid.timeId.*.*",
//                                                         "PULL", false ,
//                                                      "nats://localhost:4222", "queueName",
//                                                            "events", "durName",
//                                                            1000, 3,  40,
//                                                        connectByteBufferSize,initialMaxWaitTimeMs, maxWaitTimeMs);
//        assertEquals(true, config.isReaderEnabled());
//        assertEquals(true, config.isPullBasedConsumer());
//        assertEquals("Events.>", config.getSubjectName());
//        assertEquals("Events.uuid.timeId.*.*", config.getFilter());
//        assertEquals(false, config.isCreateEventStream());
//        assertEquals("nats://localhost:4222", config.getUrl());
//        assertEquals("queueName", config.getQueueName());
//        assertEquals("events", config.getStreamName());
//        assertEquals("durName", config.getDurableName());
//        assertEquals(1000, config.getBatchSize());
//        assertEquals(3, config.getNumberOfReplicas());
//        assertEquals(40, config.getNumberOfConsumers());
//        assertEquals(20*1024*1024, config.getConnectionByteBufferSize());
//        assertEquals(1000, config.getInitialMaxWaitTimeMs());
//        assertEquals(100, config.getMaxWaitTimeMs());
//
//    }
}