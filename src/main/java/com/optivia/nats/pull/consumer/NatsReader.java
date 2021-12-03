package com.optivia.nats.pull.consumer;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * NatsReader reads messages from NATS server. It reads a batch of messages
 * to prevent it bloating the service as there are  millions of Event
 * messages stored in NATS.
 *
 * Method
 * consume() is the API call to extract the messages from NATS and store in a container
 * getMessages() is the API call to return a reference to the container of Message objects
 *
 */
public final class NatsReader implements Supplier<List<Message>> {
    private static final Logger logger = LogManager.getLogger(NatsReader.class);
    private static final int MAX_ALLOWED_BATCH_SIZE = 256;
    public static final int MAX_ACK_PENDING = -1;
    private final NatsConfiguration configuration;
    private int totalRead;
    private boolean noMoreMsgs;

    public NatsReader(NatsConfiguration configuration) {
        this.configuration = configuration;
        totalRead = 0;
        noMoreMsgs =  true;
    }
    /**
     * Consumes up to fetchBatchSize messages from NATS.
     * Populates the Messages extracted into a container, see getMessages API
     * call to return these Messages.
     *
     * @return list of the message consumed from NATS
     */
    public List<Message> get() {
        Options options = new Options.Builder().
                server(configuration.getUrl()).
                connectionListener(new NatsConnectionListener(this)).
                reconnectBufferSize(configuration.getConnectionByteBufferSize()).  // Set buffer in bytes
                        build();
        List<Message> messages = new ArrayList<>();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = getJetStream(nc, jsm);
            consumeMessages(js, messages);
        } catch (IOException e) {
            logger.error("I/O error communicating to the NATS server.", e);
        } catch (InterruptedException | JetStreamApiException | TimeoutException e) {
            logger.error("Processing JetStream messages error.", e);
        }
        return messages;
    }
    /**
     * Has Nats indicated that no more messages to read
     *
     * @return true if no more messages to read otherwise false
     */
    public boolean noMoreMsgs() {
      return noMoreMsgs;
    }
    /**
     * Get the total message this consumer has gotten from the NATS server
     *
     * @return total number of messages consumer by  this subscriber.
     */
    public int getTotalRead() {
        return totalRead;
    }
    /**
     * Create a Pull subscriber using the durable name.
     *
     * @param js
     * @return handle to the pull consumer.
     * @throws JetStreamApiException
     * @throws IOException
     */
    private JetStreamSubscription getPullSubscribeOptions(JetStream js) throws JetStreamApiException, IOException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(configuration.getDurableName())
                .maxAckPending(MAX_ACK_PENDING) // as we are pull we should ste to -1 to allow us to scale out
                .build();
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
        /*
         * Consume all the events in the stream
         */
        return js.subscribe(configuration.getSubjectName(), pullOptions);
    }
    /**
     * Create a pull subscriber with a durableName and the filter subject set.
     * @param js
     * @return handle to the filtered pull consumer.
     * @throws JetStreamApiException
     * @throws IOException
     */
    private JetStreamSubscription getFilteredPullSubscribeOptions(JetStream js) throws JetStreamApiException, IOException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .filterSubject(configuration.getFilter())
                .durable(configuration.getDurableName())
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
     * @param js
     * @param messages
     * @throws JetStreamApiException
     * @throws IOException
     */
    private void consumeMessages(JetStream js, List<Message> messages) throws JetStreamApiException, IOException {
        JetStreamSubscription pullSub;
        if (configuration.getFilter() != null &&
                configuration.getFilter().length() > 0) {
            pullSub = getFilteredPullSubscribeOptions(js);
        } else {
            pullSub = getPullSubscribeOptions(js);
        }
        /*
         * dont block waiting on the complete batch size of messages
         * if we receive less than batch size then we should return
         *
         * no point polling nats
         */
        try {
            boolean noMoreToRead = false;
            do {
                /*
                 * NATS mandates batch size of 256 messages max in a batch if the fetch size is >
                 * then we need to chunk the response from NATS and grab it in chunks of 256
                 */
                int batchSize = calculateBatchSize();
                pullSub.pullNoWait(batchSize);
                /*
                 * if noMoreToRead false indicates that we have exhausted this batch and there might be more
                 * to retrieve from NATS
                 * true means that NATS has sent a 404 indicating no more messages at this time
                 * lets exit, regardless
                 */
                noMoreToRead = fetchBatch(messages, pullSub);
            } while (noMoreToRead == false && totalRead < configuration.getBatchSize());
        } catch (InterruptedException e) {
            logger.error("Error reading message of the NATS server.",e);
        }
    }
    /**
     * fetchBatch
     * fetches a batch of messages from NATS
     *
     * @return true iff NATS has no more messages available.
     *@param messages
     * @param pullSub
     */
    private  boolean fetchBatch(List<Message> messages, Subscription pullSub) throws InterruptedException {
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
                messages.add(msg);
                totalRead++;
                msg.ack();
                /*
                 * message should be here.
                 */
                msg = pullSub.nextMessage(Duration.ofMillis(configuration.getMaxWaitTimeMs()));
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
     * @param nc
     * @param jsm
     * @return
     * @throws JetStreamApiException
     * @throws IOException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private JetStream getJetStream(Connection nc, JetStreamManagement jsm) throws
            JetStreamApiException, IOException, InterruptedException, TimeoutException {
        /*
         * Perhaps we should drive this from the command line to build the
         * stream up front using nats.cli.
         */
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(configuration.getStreamName())
                .subjects(configuration.getSubjectName())
                .storageType(StorageType.File)
                .replicas(configuration.getNumberOfReplicas())
                .build();
        // Create the stream
        StreamInfo streamInfo = getStreamInfo(jsm, configuration.getStreamName(), false);
        if (streamInfo == null) {
            jsm.addStream(streamConfig);
        }
        JetStream js = nc.jetStream();
        nc.flush(Duration.ofSeconds(1));
        return js;
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
}
