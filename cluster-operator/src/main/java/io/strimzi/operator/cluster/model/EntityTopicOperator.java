/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the Topic Operator deployment
 */
public class EntityTopicOperator extends AbstractModel {
    protected static final String APPLICATION_NAME = "entity-topic-operator";

    protected static final String TOPIC_OPERATOR_CONTAINER_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-entity-topic-operator";
    private static final String CERT_SECRET_KEY_NAME = "entity-operator";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Topic Operator configuration keys
    public static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    public static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String ENV_VAR_SECURITY_PROTOCOL = "STRIMZI_SECURITY_PROTOCOL";

    public static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withInitialDelaySeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT).build();

    // Volume name of the temporary volume used by the TO container
    // Because the container shares the pod with other containers, it needs to have unique name
    /*test*/ static final String TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-to-tmp";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    /* test */ String kafkaBootstrapServers;
    /* test */ String zookeeperConnect;

    private String watchedNamespace;
    /* test */ int reconciliationIntervalMs;
    /* test */ int zookeeperSessionTimeoutMs;
    /* test */ String resourceLabels;
    /* test */ int topicMetadataMaxAttempts;
    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    /**
     * @param reconciliation   The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected EntityTopicOperator(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = cluster + NAME_SUFFIX;
        this.readinessPath = "/";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;

        // new KafkaStreamsTopicStore needs a bit more to start
        this.startupProbeOptions = new ProbeBuilder()
                .withPeriodSeconds(10)
                .withFailureThreshold(12)
                .build();

        // create a default configuration
        this.kafkaBootstrapServers = KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT;
        this.zookeeperConnect = "localhost:" + EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
        this.reconciliationIntervalMs = EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);
        this.topicMetadataMaxAttempts = EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigMapName = KafkaResources.entityTopicOperatorLoggingConfigMapName(cluster);
        this.logAndMetricsConfigVolumeName = "entity-topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/tmp/topic-operator/custom-config/";
        this.logAndMetricsRackConfigVolumeName = "entity-topic-operator-metrics-and-logging-rack";
        this.logAndMetricsRackConfigMountPath = "/opt/topic-operator/custom-config/";
    }

    /**
     * Create an Entity Topic Operator from given desired resource. When Topic Operator (Or Entity Operator) are not
     * enabled, it returns null.
     *
     * @param reconciliation The reconciliation
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Topic Operator one
     *
     * @return Entity Topic Operator instance, null if not configured
     */
    public static EntityTopicOperator fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly) {
        if (kafkaAssembly.getSpec().getEntityOperator() != null
                && kafkaAssembly.getSpec().getEntityOperator().getTopicOperator() != null) {
            EntityTopicOperatorSpec topicOperatorSpec = kafkaAssembly.getSpec().getEntityOperator().getTopicOperator();
            EntityTopicOperator result = new EntityTopicOperator(reconciliation, kafkaAssembly);

            result.setOwnerReference(kafkaAssembly);
            String image = topicOperatorSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE, "quay.io/strimzi/operator:latest");
            }
            result.image = image;
            result.watchedNamespace = topicOperatorSpec.getWatchedNamespace() != null ? topicOperatorSpec.getWatchedNamespace() : kafkaAssembly.getMetadata().getNamespace();
            result.reconciliationIntervalMs = topicOperatorSpec.getReconciliationIntervalSeconds() * 1_000;
            result.zookeeperSessionTimeoutMs = topicOperatorSpec.getZookeeperSessionTimeoutSeconds() * 1_000;
            result.topicMetadataMaxAttempts = topicOperatorSpec.getTopicMetadataMaxAttempts();
            result.setLogging(topicOperatorSpec.getLogging());
            result.setGcLoggingEnabled(topicOperatorSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : topicOperatorSpec.getJvmOptions().isGcLoggingEnabled());
            result.setJvmOptions(topicOperatorSpec.getJvmOptions());
            result.setResources(topicOperatorSpec.getResources());
            if (topicOperatorSpec.getStartupProbe() != null) {
                result.setStartupProbe(topicOperatorSpec.getStartupProbe());
            }
            if (topicOperatorSpec.getReadinessProbe() != null) {
                result.setReadinessProbe(topicOperatorSpec.getReadinessProbe());
            }
            if (topicOperatorSpec.getLivenessProbe() != null) {
                result.setLivenessProbe(topicOperatorSpec.getLivenessProbe());
            }

            return result;
        } else {
            return null;
        }
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        return List.of(new ContainerBuilder()
                .withName(TOPIC_OPERATOR_CONTAINER_NAME)
                .withImage(getImage())
                .withArgs("/opt/strimzi/bin/topic_operator_run.sh")
                .withEnv(getEnvVars())
                .withPorts(List.of(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withStartupProbe(ProbeGenerator.httpProbe(startupProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME))
                .withLivenessProbe(ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath + "healthy", HEALTHCHECK_PORT_NAME))
                .withReadinessProbe(ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath + "ready", HEALTHCHECK_PORT_NAME))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withLifecycle(new LifecycleBuilder().withNewPostStart().withNewExec()
                        .withCommand("/bin/bash", "-c", "sleep 60; a=1; b=0;while [ 200 -ne `curl -I -m 5 -s -w \"%{http_code}\\n\"" +
                                " -o /dev/null  localhost:8080/ready`  ];do if [ $a -eq 10 ]; then break;" +
                                "fi;sleep 2; b=1; a=$[$a+1] ; done; if [ $b -eq 0 ]; then " +
                                "echo \"\" > /etc/eto-certs/*.key; " +
                                "echo \"\" > /etc/tls-sidecar/cluster-ca-certs/*.password; echo \"\" > /etc/eto-certs/*.password; fi;")
                        .endExec().endPostStart().build())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(buildEnvVar(ENV_VAR_SECURITY_PROTOCOL, EntityTopicOperatorSpec.DEFAULT_SECURITY_PROTOCOL));
        varList.add(buildEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        ModelUtils.javaOptions(varList, getJvmOptions());

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    public List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>(2);
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
        volumeList.add(VolumeUtils.createEmptyDirVolume(logAndMetricsRackConfigVolumeName, "10Mi", "Memory"));
        return volumeList;
    }

    private List<VolumeMount> getVolumeMounts() {
        return List.of(createTempDirVolumeMount(TOPIC_OPERATOR_TMP_DIRECTORY_DEFAULT_VOLUME_NAME),
                VolumeUtils.createVolumeMount(logAndMetricsRackConfigVolumeName, logAndMetricsRackConfigMountPath),
                VolumeUtils.createVolumeMount(EntityOperator.ETO_CERTS_RACK_VOLUME_NAME, EntityOperator.ETO_CERTS_RACK_VOLUME_MOUNT),
                VolumeUtils.createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_TOPIC_RACK_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_RACK_VOLUME_MOUNT));
    }

    @Override
    protected String getRoleName() {
        return KafkaResources.entityOperatorDeploymentName(cluster);
    }

    public RoleBinding generateRoleBindingForRole(String namespace, String watchedNamespace) {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(KafkaResources.entityOperatorDeploymentName(cluster))
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName(getRoleName())
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .build();

        RoleBinding rb = generateRoleBinding(
                KafkaResources.entityTopicOperatorRoleBinding(cluster),
                watchedNamespace,
                roleRef,
                List.of(ks)
        );

        // We set OwnerReference only within the same namespace since it does not work cross-namespace
        if (!namespace.equals(watchedNamespace)) {
            rb.getMetadata().setOwnerReferences(Collections.emptyList());
        }

        return rb;
    }

    /**
     * Transforms properties to log4j2 properties file format and adds property for reloading the config
     * @param properties map with properties
     * @return modified string with monitorInterval
     */
    @Override
    public String createLog4jProperties(OrderedProperties properties) {
        if (!properties.asMap().containsKey("monitorInterval")) {
            properties.addPair("monitorInterval", "30");
        }
        return super.createLog4jProperties(properties);
    }

    /**
     * Generate the Secret containing the Entity Topic Operator certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Entity Topic Operator private key.
     *
     * Note: This certificate will be used by both Topic Operator Container and the TLS sidecar container. The User Operator Container use a separate certificate.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Secret secret = clusterCa.entityTopicOperatorSecret();
        return ModelUtils.buildSecret(reconciliation, clusterCa, secret, namespace, KafkaResources.entityTopicOperatorSecretName(cluster), name,
            CERT_SECRET_KEY_NAME, labels, createOwnerReference(), isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * @return Returns the namespace watched by the Topic Operator
     */
    public String watchedNamespace() {
        return watchedNamespace;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "entityTopicOperatorDefaultLoggingProperties";
    }

    @Override
    public String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }
}
