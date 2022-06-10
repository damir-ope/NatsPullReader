package com.optivia.nats.pull.consumer.connection;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;

public class NatsConnectionManager {

  private static Connection connection = null;

  public static synchronized Connection getConnection() {
    if (connection != null) {
      return connection;
    }
    Options options = new Options.Builder()
        .server("localhost:4222")
        .connectionListener(new NatsConnectionListener())
        .reconnectBufferSize(20 * 1024 * 1024) // Set buffer in bytes
        .build();
    try {
      connection = Nats.connect(options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return connection;
  }
}
