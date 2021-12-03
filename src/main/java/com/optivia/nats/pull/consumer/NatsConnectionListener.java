package com.optivia.nats.pull.consumer;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Automatically reconnects to the endpoint if TCP connection drops.
 * Also buffers any messages received internally if connection drops that havent
 * been processed yet.
 *
 * It can be used to detect slow consumers and flow control back pressure if required
 * however that is mainly an issue if we are using push consumers (ASynchronous)
 *
 * We can build this out as necessary.
 */
public final class NatsConnectionListener implements ConnectionListener {
    private static final Logger logger = LogManager.getLogger(NatsConnectionListener.class);
    private final NatsReader app;
    /*
     * Reference to the reader if needs anything
     * @param app reader application
     */
    public NatsConnectionListener(final NatsReader app) {
        this.app = app;
    }
    @Override
    public void connectionEvent(Connection nc, Events event) {
        if (event == Events.DISCOVERED_SERVERS) {
            logger.info("Known server: {} ", nc.getServers());
        }
    }
}
