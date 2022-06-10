package com.optivia.nats.pull.consumer;

import com.optivia.nats.pull.consumer.config.NatsEhafConsumerConfig;
import com.optivia.nats.pull.consumer.connection.NatsConnectionManager;
import com.optivia.nats.pull.consumer.message.EventMessage;
import io.nats.client.JetStreamApiException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultipleEventStreamReader {

  private static final Integer initialMaxWaitTimeMs = 1000;
  private static final Integer maxWaitTimeMs = 100;

  private static final Logger logger = LogManager.getLogger(SingleEventStreamReader.class);

  //<partition>.<timeId>.<useId>.<RootCustomerId>
  public static final String FULL_SUBJECT_FORMAT = "%s.%s.%s.%s";
  public static final String SUBJECT_NAME_WITH_WILDCARD = "%s.>";
  public static final String STREAM_NAME = "Events%s";


  public static void main(String[] args)
      throws InterruptedException, JetStreamApiException, IOException {

    final String consumerDurableName = "STEH-EHAF" + UUID.randomUUID();
    final String subjectFilter = getSubjectFilter();
    final String subjectName = getSubjectName();
    final String streamName = getStreamName();

    NatsEhafConsumerConfig config = new NatsEhafConsumerConfig(subjectName,
        subjectFilter,
        consumerDurableName, 1000,
        initialMaxWaitTimeMs, maxWaitTimeMs);
    NatsReader natsReader = new NatsReader(config);

    readMessages(natsReader);

    natsReader.deleteConsumerOfStream(streamName, consumerDurableName);

    NatsConnectionManager.getConnection().close();
  }

  private static String getStreamName() {
    // calculate custHashId from customerId
    final String subsHashId = "0";
    return String.format(STREAM_NAME, subsHashId);
  }

  private static String getSubjectName() {
    // calculate custHashId from customerId
    final String subsHashId = "0";
    return String.format(SUBJECT_NAME_WITH_WILDCARD, subsHashId);

  }

  private static String getSubjectFilter() {
    // calculate custHashId from customerId
    final String subsHashId = "0";
    final String timeId = "*";
    final String useId = "22";
    final String rootCustomerId = "451952505";

    return String.format(FULL_SUBJECT_FORMAT, subsHashId, timeId, useId, rootCustomerId);
  }

  private static void readMessages(final NatsReader natsReader)
      throws JetStreamApiException, IOException, InterruptedException {
    final List<EventMessage> eventMessages = natsReader.getMessages();
    System.out.println("num of msgs " + eventMessages.size());
    final EventMessage msg = eventMessages.isEmpty() ? null : eventMessages.get(0);
    System.out.println("json " + msg);

  }

}
