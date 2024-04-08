/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.StrimziClientResources;
import io.strimzi.api.kafka.model.StrimziClientSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.template.StrimziClientTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.*;

/**
 * Kafka Exporter model
 */
public class StrimziClient extends AbstractModel {
    protected static final String COMPONENT_TYPE = "strimzi-client";

    // Configuration for mounting certificates
    protected static final String STRIMZI_CLIENT_USER_CERTS_VOLUME_NAME = "strimzi-client-user-certs";
    protected static final String STRIMZI_CLIENT_USER_CERTS_VOLUME_MOUNT = "/etc/strimzi-client/strimzi-client-user-certs/";
    protected static final String CLUSTER_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/etc/strimzi-client/cluster-ca-certs/";

    // Configuration defaults
    /*test*/ static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_TIMEOUT = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_PERIOD = 30;
    private static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).withPeriodSeconds(DEFAULT_HEALTHCHECK_PERIOD).build();

    protected static final String ENV_VAR_STRIMZI_CLIENT_KAFKA_VERSION = "STRIMZI_CLIENT_KAFKA_VERSION";
    protected static final String ENV_VAR_STRIMZI_CLIENT_KAFKA_SERVER = "STRIMZI_CLIENT_KAFKA_SERVER";
    protected static final String ENV_VAR_STRIMZI_CLIENT_AUTHENTICATION_TYPE = "STRIMZI_CLIENT_AUTHENTICATION_TYPE";

    protected static final String CO_ENV_VAR_CUSTOM_STRIMZI_CLIENT_POD_LABELS = "STRIMZI_CUSTOM_STRIMZI_CLIENT_LABELS";

    protected String authenticationType;
    protected String version;

    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_STRIMZI_CLIENT_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected StrimziClient(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, StrimziClientResources.deploymentName(resource.getMetadata().getName()), COMPONENT_TYPE);

        this.replicas = 1;
        this.readinessPath = "/healthz";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
        this.livenessPath = "/healthz";
        this.livenessProbeOptions = READINESS_PROBE_OPTIONS;

        this.mountPath = "/var/lib/kafka";

        // Kafka Exporter is all about metrics - they are always enabled
        this.isMetricsEnabled = true;

    }

    /**
     * Builds the KafkaExporter model from the Kafka custom resource. If KafkaExporter is not enabled, it will return null.
     *
     * @param reconciliation    Reconciliation marker for logging
     * @param kafkaAssembly     The Kafka CR
     * @param versions          The list of supported Kafka versions
     *
     * @return                  KafkaExporter model object when Kafka Exporter is enabled or null if it is disabled.
     */
    public static StrimziClient fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        StrimziClientSpec spec = kafkaAssembly.getSpec().getStrimziClient();

        if (spec != null) {
            StrimziClient kafkaExporter = new StrimziClient(reconciliation, kafkaAssembly);

            kafkaExporter.setResources(spec.getResources());

            if (spec.getReadinessProbe() != null) {
                kafkaExporter.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                kafkaExporter.setLivenessProbe(spec.getLivenessProbe());
            }

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_STRIMZI_CLIENT_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            kafkaExporter.setImage(image);

            if (spec.getTemplate() != null) {
                StrimziClientTemplate template = spec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                    kafkaExporter.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    kafkaExporter.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
                }

                if (template.getContainer() != null) {
                    if (template.getContainer().getEnv() != null) {
                        kafkaExporter.templateContainerEnvVars = template.getContainer().getEnv();
                    }
                    if (template.getContainer().getSecurityContext() != null) {
                        kafkaExporter.templateContainerSecurityContext = template.getContainer().getSecurityContext();
                    }
                }

                if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                    kafkaExporter.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                    kafkaExporter.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
                }

                ModelUtils.parseDeploymentTemplate(kafkaExporter, template.getDeployment());
                ModelUtils.parsePodTemplate(kafkaExporter, template.getPod());
                kafkaExporter.templatePodLabels = Util.mergeLabelsOrAnnotations(kafkaExporter.templatePodLabels, DEFAULT_POD_LABELS);
            }

            kafkaExporter.version = versions.supportedVersion(kafkaAssembly.getSpec().getKafka().getVersion()).version();

            return kafkaExporter;
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        return portList;
    }

    /**
     * Generates Kafka Exporter Deployment
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image Pull Secrets
     *
     * @return  Generated deployment
     */
    public Deployment generateDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return createDeployment(
                getDeploymentStrategy(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets,
                securityProvider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(templateSecurityContext))
        );
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>(1);

        Container container = new ContainerBuilder()
                .withName(componentName)
                .withImage(getImage())
                .withCommand("/opt/strimzi-client/strimzi_client_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withResources(getResources())
                .withVolumeMounts(createTempDirVolumeMount(),
//                        VolumeUtils.createVolumeMount(STRIMZI_CLIENT_USER_CERTS_VOLUME_NAME, STRIMZI_CLIENT_USER_CERTS_VOLUME_MOUNT),
                        VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME_NAME, CLUSTER_CA_CERTS_VOLUME_MOUNT))
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(securityProvider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainerSecurityContext)))
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_STRIMZI_CLIENT_KAFKA_SERVER, version));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_CLIENT_KAFKA_SERVER, KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_CLIENT_AUTHENTICATION_TYPE, "tls"));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(3);

        volumeList.add(createTempDirVolume());
//        volumeList.add(VolumeUtils.createSecretVolume(STRIMZI_CLIENT_USER_CERTS_VOLUME_NAME, StrimziClientResources.secretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(CLUSTER_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));

        return volumeList;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    @Override
    protected String getServiceAccountName() {
        return StrimziClientResources.serviceAccountName(cluster);
    }
}
