/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code StrimziClient} instance.
 */
public class StrimziClientResources {
    protected StrimziClientResources() { }

    /**
     * Returns the name of the Strimzi Client {@code Deployment}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Strimzi Client {@code Deployment}.
     */
    public static String deploymentName(String kafkaClusterName) {
        return kafkaClusterName + "-strimzi-client";
    }

    /**
     * Returns the name of the Strimzi Client {@code ServiceAccount}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Strimzi Client {@code ServiceAccount}.
     */
    public static String serviceAccountName(String kafkaClusterName) {
        return deploymentName(kafkaClusterName);
    }

    /**
     * Returns the name of the Prometheus {@code Service}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String kafkaClusterName) {
        return kafkaClusterName + "-strimzi-client";
    }

    /**
     * Returns the name of the Strimzi Client {@code Secret} for a {@code Kafka} cluster of the given name.
     * This {@code Secret} will only exist if {@code Kafka.spec.strimziClient} is configured in the
     * {@code Kafka} resource with the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Strimzi Client {@code Secret}.
     */
    public static String secretName(String clusterName) {
        return deploymentName(clusterName) + "-certs";
    }
}
