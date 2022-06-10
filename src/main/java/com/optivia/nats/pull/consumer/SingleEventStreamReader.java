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

public class SingleEventStreamReader {

  private static final Integer initialMaxWaitTimeMs = 1000;
  private static final Integer maxWaitTimeMs = 100;

  private static final Logger logger = LogManager.getLogger(SingleEventStreamReader.class);

  //Events.<SubscriberHashId>.<timeId>.<useId>.<RootCustomerId>
  public static final String FULL_SUBJECT_FORMAT = "Events.%s.%s.%s.%s";
  public static final String SUBJECT_NAME_WITH_WILDCARD = "Events.>";
  public static final String STREAM_NAME = "Events";


  public static void main(String[] args)
      throws InterruptedException, JetStreamApiException, IOException {

    final String consumerDurableName = "STEH-EHAF" + UUID.randomUUID();
    final String subjectFilter = getSubjectFilter();

    NatsEhafConsumerConfig config = new NatsEhafConsumerConfig(SUBJECT_NAME_WITH_WILDCARD,
        subjectFilter,
        consumerDurableName, 1000,
        initialMaxWaitTimeMs, maxWaitTimeMs);
    NatsReader natsReader = new NatsReader(config);

//    natsReader.createJetStream();

    readMessages(natsReader);
//
//    natsReader.getConsumersOfStream(STREAM_NAME).forEach(System.out::println);
//    natsReader.deleteConsumerOfStream(STREAM_NAME, consumerDurableName);

//    JetStream js = NatsConnectionManager.getConnection().jetStream();
//    natsReader.createConsumer(js);
    natsReader.deleteConsumerOfStream(STREAM_NAME, consumerDurableName);

    NatsConnectionManager.getConnection().close();
  }

  private static String getSubjectFilter() {
    // calculate custHashId from customerId
    final String subsHashId = "102";
    final String timeId = "*";
    final String useId = "1";
    final String rootCustomerId = "38598123456";

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
