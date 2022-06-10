package com.optivia.nats.pull.consumer.config;

public final class NatsEhafConsumerConfig {
    private final String subjectName;
    private final String filter;
    private final String durableName;
    private final Integer batchSize;
    private final Integer initialMaxWaitTimeMs;
    private final Integer maxWaitTimeMs;

    public NatsEhafConsumerConfig(final String subjectName, final String filter, final String durableName,
        final Integer batchSize,
        final Integer initialMaxWaitTimeMs, final Integer maxWaitTimeMs) {
        this.subjectName = subjectName;
        this.filter = filter;
        this.durableName = durableName;
        this.batchSize = batchSize;
        this.initialMaxWaitTimeMs = initialMaxWaitTimeMs;
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    public final String getSubjectName() {
        return subjectName;
    }

    public final String getFilter() {
        return filter;
    }

    public final String getDurableName() {
        return durableName;
    }

    public final int getBatchSize() {
        return batchSize;
    }

    /**
     * Get the initial wait time in ms for pull from nats
     *
     * @return initial wait time in milliseconds
     */
    public final int getInitialMaxWaitTimeMs() {
        return initialMaxWaitTimeMs;
    }
    /**
     * Get the wait time in ms for pull from nats
     *
     * @return wait time in milliseconds
     */
    public final int getMaxWaitTimeMs() {
        return maxWaitTimeMs;
    }

}
