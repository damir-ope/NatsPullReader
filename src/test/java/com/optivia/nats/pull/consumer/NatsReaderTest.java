package com.optivia.nats.pull.consumer;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import static org.junit.jupiter.api.Assertions.assertEquals;
/**
 * Test harness for the NatsReader consumer logic
 */
class NatsReaderTest {

    private String subjectName;
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
    @Test
    public void testNoServerListeningOnPort() throws IOException, InterruptedException {

        NatsReaderConfiguration config = new NatsReaderConfiguration(true, "Events.>",
                "Events.uuid.timeId.*.*",
                "PULL", true ,
                "nats://localhost:4222", "queueName",
                "events", "durName",
                1000, 3,  40, connectByteBufferSize,
                initialMaxWaitTimeMs, maxWaitTimeMs);
        NatsReader reader = new NatsReader(config);
        List<Message> msgs = reader.get();
        System.out.println("testNoServerListeningOnPort: messages["+reader.getTotalRead()+"]");
        assertEquals(0, reader.getTotalRead());

        assertEquals(true, msgs.isEmpty());
    }

    @Test
    public void testDefaultConnectionNoMessagesInNats() throws IOException, InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                                                    true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 10, there are ZERO messages in the NATS stream
             * So client will get 404 from NATS to indicate no more
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    10, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            List<Message> msgs = reader.get();
            assertEquals(true,msgs.isEmpty());
            /**
             * NATS has NO more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    void testConsumeNatsOneMessage() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");

            /**
             * batch size of 10, and we will populate 1 messages into the stream
             * However as only 1 message in stream it will return just 1 to the client
             * 404 will be set to indicate no more
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    10, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);

            publish("subj.test.10.20", "AweePrefix", 1,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(1, msgs.size());
            /**
             * NATS has more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }

    }
    @Test
    void testConsumeAllNatsMessages() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");

            /**
             * batch size of 10 and we will populate 10 messages into the stream.
             *
             * This means NATS client will pull all 10 messages required in 1 batch, 'au point'
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    10, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            publish("subj.test.10.20", "AweePrefix", 10,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(10,msgs.size());
            /**
             * NATS has more messages to consume, well it doesnt really
             * but it satisfied the batch without needing to send 404
             */
            assertEquals(false, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }
    @Test
    void testConsumeNatsMessagesCountGreaterThanBatchSize() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 10, and we will populate 100 messages into the stream.
             *
             * This means NATS client will pull all 10 messages required in 1 batch, 'au point'
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    10, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);

            publish("subj.test.10.20", "AweePrefix", 100,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(10, msgs.size());
            /**
             * NATS has more messages to consume
             */
            assertEquals(false, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }
    @Test
    void testConsumeNatsMessagesCountLessThanBatchSize() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 100, and we will only populate 10 messages into the stream.
             *
             * This means NATS client will pull only 10 messages available in 1 batch, and indicate
             * no more available i.e. 404
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    100, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);

            publish("subj.test.10.20", "AweePrefix", 10,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(10, msgs.size());
            /**
             * NATS has no more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());

        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }
    @Test
    void testConsumeNatsMessagesCountGreaterThanMaxBatchSize() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 300, and we will populate 300 messages into the stream.
             *
             * Nats Max Batch Size is 256 message, there we will require 2 iterations
             * or batches to extract all the messages
             *
             * This means NATS client will pull all 300 messages required in 2 batches, 'au point'
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    300, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            publish("subj.test.10.20", "AweePrefix", 300,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(300, msgs.size());
            assertEquals(300, reader.getTotalRead());
            /**
             * NATS has more messages to consume
             */
            assertEquals(false, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }
    @Test
    void testConsumeNatsMessagesCountEqualToMaxBatchSize() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 256, and we will populate 256 messages into the stream.
             *
             * Nats Max Batch Size is 256 message, there we will require 1 iteration
             * or batches to extract all the messages
             *
             * This means NATS client will pull all 256 messages required in 1 batch, 'au point'
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    256, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);

            publish("subj.test.10.20", "AweePrefix", 300,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(256, msgs.size());
            /**
             * NATS has more messages to consume
             */
            assertEquals(false, reader.noMoreMsgs());
            assertEquals(256, reader.getTotalRead());

        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testConsumeLessThanBatchStopAfterThreeIterationsD() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * batch size of 700, and we will populate 678 messages into the stream.
             *
             * Nats Max Batch Size is 256 message, therefore we will require 3 iterations
             * or batches to extract all the messages
             *
             * This means NATS client will pull all 678 messages required in 3 batches,
             * and return less than number asked for as there is no more messages in NATS
             * at this time.
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    700, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            publish("subj.test.10.20", "AweePrefix", 678,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(678, msgs.size());
            assertEquals(678, reader.getTotalRead());
            /**
             * NATS has NO more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());

        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCallConsumeTwiceToFulfillBatch() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * First Iteration of Consume
             * ===========================
             * Similar to above test fetch batch size of 700 requested, and we
             * will initially only populate 678 messages into the stream.
             *
             * Nats Max Batch Size is 256 message, therefore we will require 3 iterations
             * or batches to extract all the messages
             *
             * This means NATS client will pull all 678 messages required in 3 batches,
             * and return less than number asked for as there is no more messages in NATS
             * at this time.
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    700, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            publish("subj.test.10.20", "AweePrefix", 678,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(678, msgs.size());
            assertEquals(678, reader.getTotalRead());
            /**
             * NATS has NO more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());

            /**
             * Second Iteration of Consume
             * ===========================
             * So first iteration retrieved 678 of the 700 messages requested. This
             * is because client correctly detected that NATS server doesn't have any more
             * messages and returns to caller.
             *
             * Now we publish 22 more message to NATS to fulfil the request of 700.
             *
             * Nats Max Batch Size is 256 message, therefore we will require 1 iteration
             * or batches to extract the 22 messages rquired.
             *
             * This means NATS client will pull all 700 messages
             *
             */
            publish("subj.test.10.20", "AweePrefix", 22,
                    "streamName", "subj.>",1, server.getURI());
            // should only consume 22 to fulfill the batch this time
            List<Message> msgs2 = reader.get();
            assertEquals(22, msgs2.size());
            assertEquals(700, reader.getTotalRead());
            /**
             * NATS has more messages to consume
             * well it doesn't but the client doesn't know that as its
             * retrieved all it needed from the batch of 22
             */
            assertEquals(false, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCallConsumeThreeTimesToFulfillBatch() throws InterruptedException {
        try (NatsServerRunner server = new NatsServerRunner(false,
                true)) {
            System.out.println("Server running on port: " +
                    server.getPort());
            System.out.println("Server URI on port: " +
                    server.getURI());
            System.out.println("Server config file" +
                    server.getConfigFile());
            deleteStream(server.getURI(),"streamName");
            /**
             * First Iteration of Consume
             * ===========================
             * Similar to above test fetch batch size of 700 requested, and we
             * will initially only populate 678 messages into the stream.
             *
             * Nats Max Batch Size is 256 message, therefore we will require 3 iterations
             * or batches to extract all the messages
             *
             * This means NATS client will pull all 678 messages required in 3 batches,
             * and return less than number asked for as there is no more messages in NATS
             * at this time.
             *
             */
            NatsReaderConfiguration config = new NatsReaderConfiguration(true, "subj.>",
                    "subj.test.*.*",
                    "PULL", true ,
                    server.getURI(), "queueName",
                    "streamName", "durName",
                    700, 1,  40, connectByteBufferSize,
                    initialMaxWaitTimeMs, maxWaitTimeMs);
            NatsReader reader = new NatsReader(config);
            publish("subj.test.10.20", "AweePrefix", 678,
                    "streamName", "subj.>",1, server.getURI());
            List<Message> msgs = reader.get();
            assertEquals(678, msgs.size());
            assertEquals(678, reader.getTotalRead());
            /**
             * NATS has NO more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());

            /**
             * Second Iteration of Consume
             * ===========================
             * So first iteration retrieved 678 of the 700 messages requested. This
             * is because client correctly detected that NATS server doesn't have anymore
             * messages and returns to caller.
             *
             * Now we publish 10 more message to NATS.
             *
             * Call consume again to consume these but still not enough to fulfill batch,
             *
             * This means NATS client will pull all 688 messages
             *
             */
            publish("subj.test.10.20", "AweePrefix", 10,
                    "streamName", "subj.>",1, server.getURI());
            // should only consume 10 as thats all available
            msgs = reader.get();
            assertEquals(10,msgs.size());
            assertEquals(688, reader.getTotalRead());
            /**
             * NATS has NO more messages to consume
             */
            assertEquals(true, reader.noMoreMsgs());

            /**
             * Third Iteration of Consume
             * ===========================
             * So first& second iteration retrieved 688 of the 700 messages requested. This
             * is because client correctly detected that NATS server doesn't have anymore
             * messages and returns to caller.
             *
             * Now we publish 100 more message to NATS.
             *
             * Call consume again to consume these will be enough to fulfill batch,
             *
             * This means NATS client will pull only 700 messages as required, leaving
             * other messages in the stream to be consumed later if required.
             *
             * Point is we are honouring the request for 700 only over multiple iterations
             * 'au point' as they say in Belfast.
             *
             */
            publish("subj.test.10.20", "AweePrefix", 100,
                    "streamName", "subj.>",1, server.getURI());
            // should only consume 12 100
            msgs = reader.get();
            assertEquals(12, msgs.size());
            assertEquals(700, reader.getTotalRead());
            /**
             * NATS has more messages to consume
             */
            assertEquals(false, reader.noMoreMsgs());
        } catch (IOException | JetStreamApiException e) {
            e.printStackTrace();
        }
    }
    void publish(String subject, String prefix, int count,
                 String streamName, String subjectName, Integer replicas,
                 String endpoint) throws IOException, JetStreamApiException {
        Options options = new Options.Builder().
                server(endpoint).
                reconnectBufferSize(20 * 1024 * 1024).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = getJetStream(nc, jsm, streamName, subjectName, replicas);

            for (int x = 1; x <= count; x++) {
                String data = prefix + x;

                Message msg = NatsMessage.builder()
                        .subject(subject)
                        .data(data.getBytes(StandardCharsets.US_ASCII))
                        .build();
                js.publish(msg);
            }
        } catch (IOException e) {
            System.out.println("Error I/O ["+e.getLocalizedMessage());
        } catch (InterruptedException | JetStreamApiException | TimeoutException e) {
            System.out.println("Error  ["+e.getLocalizedMessage());
        }
    }
    private JetStream getJetStream(Connection nc, JetStreamManagement jsm,
                                   String streamName, String subjectName, int replicas) throws
            JetStreamApiException, IOException, InterruptedException, TimeoutException {
        // Build the configuration
        /**
         * Perhaps we should drive this from the command line to build the
         * stream up front using nats.cli.
         */
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subjectName)
                .storageType(StorageType.File)
                .replicas(replicas)
                .build();
        // Create the stream

        StreamInfo streamInfo = getStreamInfo(jsm, streamName, false);
        if (streamInfo == null) {
            jsm.addStream(streamConfig);
        }
        JetStream js = nc.jetStream();
        nc.flush(Duration.ofSeconds(1));
        return js;
    }
    private StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName, boolean deleteStr) {
        StreamInfo strDetails = null;
        try {
            if ((strDetails = jsm.getStreamInfo(streamName)) != null && deleteStr) {
                // delete the stream
                jsm.deleteStream(streamName);
                strDetails = null;
            }
        }
        catch (JetStreamApiException | IOException jsae) {
            return null;
        }
        return strDetails;
    }
    public void deleteStream(String endpoint, String streamName) {
        Options options = new Options.Builder().
                server(endpoint).
                reconnectBufferSize(20 * 1024 * 1024).  // Set buffer in bytes
                        build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            getStreamInfo(jsm, streamName,  true);
        } catch (IOException e) {
        } catch (InterruptedException jsa) {
        }
    }
}