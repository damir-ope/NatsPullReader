package com.optivia.nats.pull.consumer;

import com.optivia.nats.pull.consumer.config.NatsEhafConsumerConfig;
import com.optivia.nats.pull.consumer.connection.NatsConnectionManager;
import com.optivia.nats.pull.consumer.message.EventMessage;
import com.optivia.nats.pull.consumer.message.EventSerialization;
import com.optivia.nats.pull.consumer.message.JsonEvent;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.ByteArrayInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * NatsReader reads messages from NATS server. It reads a batch of messages
 * to prevent it bloating the service as there are  millions of Event
 * messages stored in NATS.
 *
 *
 */
public final class NatsReader {
    private static final Logger logger = LogManager.getLogger(NatsReader.class);
    private static final int MAX_ALLOWED_BATCH_SIZE = 256;
    public static final int MAX_ACK_PENDING = -1;
    private final NatsEhafConsumerConfig configuration;
    private int totalRead;
    private boolean noMoreMsgs;

    private final EventSerialization event;
    public NatsReader(NatsEhafConsumerConfig configuration) {
        this.configuration = configuration;
        totalRead = 0;
        noMoreMsgs =  true;
        event = new JsonEvent();
    }
    /**
     * Consumes up to fetchBatchSize messages from NATS.
     * Populates the Messages extracted into a container, see getMessages API
     * call to return these Messages.
     *
     * @return list of the message consumed from NATS
     */
    public List<EventMessage> getMessages() throws IOException, JetStreamApiException {
        JetStreamSubscription jetStreamSubscription = createConsumer(getNatsConnection().jetStream());
        return consumeMessages(jetStreamSubscription);
    }

    /**
     *
     * @return nats connection
     *
     */
    private Connection getNatsConnection() {
        return NatsConnectionManager.getConnection();
    }

    /**
     * Create a pull subscriber with a durableName and the filter subject set.
     * @param js
     * @return handle to the filtered pull consumer.
     * @throws JetStreamApiException
     * @throws IOException
     */
    public JetStreamSubscription createConsumer(JetStream js) throws JetStreamApiException, IOException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .filterSubject(configuration.getFilter())
                .durable(configuration.getDurableName())
                .deliverPolicy(DeliverPolicy.All)
                .replayPolicy(ReplayPolicy.Instant)
                .maxAckPending(MAX_ACK_PENDING) // as we are pull we should ste to -1 to allow us to scale out
                .build();
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
        logger.info("filter-consumer filtered on {} ", configuration.getFilter());
        /*
         * Consume all the events in the stream
         */
        return js.subscribe(configuration.getSubjectName(), pullOptions);
    }

    /**
     * Consume the message from NATS.
     *
     * May require more than one iteration if the size is greater than the max size (256) of
     * a NATS pull batch.
     *
     * @param jetStreamSubscription
     *
     * @return list of messages received from stream
     */
    private List<EventMessage> consumeMessages(JetStreamSubscription jetStreamSubscription) {
        List<EventMessage> messages = new ArrayList<>();

        /*
         * dont block waiting on the complete batch size of messages
         * if we receive less than batch size then we should return
         *
         * no point polling nats
         */
        try {
            boolean noMoreToRead;
            do {
                /*
                 * NATS mandates batch size of 256 messages max in a batch if the fetch size is >
                 * then we need to chunk the response from NATS and grab it in chunks of 256
                 */
                int batchSize = calculateBatchSize();
                jetStreamSubscription.pullNoWait(batchSize);
                /*
                 * if noMoreToRead false indicates that we have exhausted this batch and there might be more
                 * to retrieve from NATS
                 * true means that NATS has sent a 404 indicating no more messages at this time
                 * lets exit, regardless
                 */
                noMoreToRead = fetchBatch(messages, jetStreamSubscription);
            } while (noMoreToRead == false && totalRead < configuration.getBatchSize());
        } catch (InterruptedException e) {
            logger.error("Error reading message of the NATS server.",e);
            Thread.currentThread().interrupt();
        }
        return messages;
    }
    /**
     * fetchBatch
     * fetches a batch of messages from NATS
     *
     * @return true iff NATS has no more messages available.
     *@param messages
     * @param pullSub
     */
    private boolean fetchBatch(List<EventMessage> messages, Subscription pullSub) throws InterruptedException {
        /*
         * wait a period for the first one
         */
        noMoreMsgs =  true;
        Message msg = pullSub.nextMessage(Duration.ofMillis(configuration.getInitialMaxWaitTimeMs()));
        if (msg !=null) {
            noMoreMsgs = false;
        }
        while (msg != null) {
            if (msg.isJetStream()) {
                try {
                    EventMessage pojo = event.deserialize(new ByteArrayInputStream(msg.getData()));
                    messages.add(pojo);
                    totalRead++;
                    msg.ack();
                    /*
                     * message should be here.
                     */
                    msg = pullSub.nextMessage(Duration.ofMillis(configuration.getMaxWaitTimeMs()));
                } catch (IOException e) {
                    logger.error("Problem serializing the Nats message to JSON, continuing", e);
                }
            } else if (msg.isStatusMessage()) {
                /*
                 * This indicates batch has nothing more to send
                 *
                 * m.getStatus().getCode should == 404
                 * m.getStatus().getMessage should be "No Messages"
                 */
                msg = null;
                //
                noMoreMsgs = true;
            }
        }
        return noMoreMsgs;
    }
    /**
     * Determines the size of a batch request sent to NATS.
     *
     * @return size of the batch to use in request to  NATS
     */
    private int calculateBatchSize() {
        int outStanding = configuration.getBatchSize() - totalRead;

        return outStanding > MAX_ALLOWED_BATCH_SIZE ? MAX_ALLOWED_BATCH_SIZE : outStanding;
    }

    /**
     * Get the JetStream handle from NATS
     *
     * @throws JetStreamApiException
     * @throws IOException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void createJetStream() throws
            JetStreamApiException, IOException, InterruptedException, TimeoutException {

        final Connection natsConnection = getNatsConnection();
            /*
             * Perhaps we should drive this from the command line to build the
             * stream up front using nats.cli.
             */
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name("Events")
                .subjects("Events.>")
                .storageType(StorageType.File)
                .replicas(1)
                .build();
            // Create the stream if it does not exist
            final JetStreamManagement jsm = natsConnection.jetStreamManagement();
            jsm.addStream(streamConfig);
            natsConnection.flush(Duration.ofSeconds(1));
            natsConnection.close();
        }
    /**
     * Get the stream information from NATS JetStream
     *
     * @param jsm
     * @param streamName
     * @param deleteStr
     * @return
     */
    private StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName, boolean deleteStr) {
        StreamInfo strDetails = null;
        try {
            strDetails = jsm.getStreamInfo(streamName);
            if (strDetails != null && deleteStr) {
                jsm.deleteStream(streamName);
                strDetails = null;
            }
        }
        catch (JetStreamApiException | IOException jsae) {
            logger.error("Error NATS Management API stream", jsae);
            return null;
        }
        return strDetails;
    }

    public List<String> getConsumersOfStream(String streamName)
        throws IOException, JetStreamApiException {
            return getNatsConnection().jetStreamManagement().getConsumerNames(streamName);
    }

    public boolean deleteConsumerOfStream(String streamName, String consumerName) {
        try  {
            return getNatsConnection()
                .jetStreamManagement()
                .deleteConsumer(streamName, consumerName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (JetStreamApiException e) {
            logger.warn(e.getMessage());
            return true;
//            throw new RuntimeException(e);
        }
    }

}
