/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.StrimziClientResources;
import io.strimzi.api.kafka.model.StrimziClientSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.api.kafka.model.template.StrimziClientTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.*;

/**
 * Strimzi Client model
 */
public class StrimziClient extends AbstractModel {
    protected static final String COMPONENT_TYPE = "strimzi-client";

    // Configuration for mounting certificates
    protected static final String STRIMZI_CLIENT_USER_CERTS_VOLUME_NAME = "strimzi-client-user-certs";
    protected static final String STRIMZI_CLIENT_USER_CERTS_VOLUME_MOUNT = "/etc/strimzi-client/strimzi-client-user-certs/";
    protected static final String CLUSTER_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/etc/strimzi-client/cluster-ca-certs/";

    protected static final String STRIMZI_CLIENT_DATA_VOLUME_NAME = "strimzi-client-data";
    protected static final String STRIMZI_CLIENT_DATA_VOLUME_MOUNT = "/opt/data";

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
    private ResourceTemplate templatePersistentVolumeClaims;
    private Storage pvcstorage;

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

        // Strimzi Client is all about metrics - they are always enabled
        this.isMetricsEnabled = true;

        this.pvcstorage = new PersistentClaimStorageBuilder()
                .withDeleteClaim(true)
                .withSize("20Gi")
                .build();

    }

    /**
     * Builds the Strimzi Client model from the Kafka custom resource. If Strimzi Client is not enabled, it will return null.
     *
     * @param reconciliation    Reconciliation marker for logging
     * @param kafkaAssembly     The Kafka CR
     * @param versions          The list of supported Kafka versions
     *
     * @return                  Strimzi Client model object when Strimzi Client is enabled or null if it is disabled.
     */
    public static StrimziClient fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        StrimziClientSpec spec = kafkaAssembly.getSpec().getStrimziClient();

        if (spec != null) {
            StrimziClient strimziClient = new StrimziClient(reconciliation, kafkaAssembly);

            strimziClient.setResources(spec.getResources());

            if (spec.getReadinessProbe() != null) {
                strimziClient.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                strimziClient.setLivenessProbe(spec.getLivenessProbe());
            }

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_STRIMZI_CLIENT_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            strimziClient.setImage(image);

            if (spec.getTemplate() != null) {
                StrimziClientTemplate template = spec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                    strimziClient.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    strimziClient.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
                }

                if (template.getContainer() != null) {
                    if (template.getContainer().getEnv() != null) {
                        strimziClient.templateContainerEnvVars = template.getContainer().getEnv();
                    }
                    if (template.getContainer().getSecurityContext() != null) {
                        strimziClient.templateContainerSecurityContext = template.getContainer().getSecurityContext();
                    }
                }

                if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                    strimziClient.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                    strimziClient.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
                }

                ModelUtils.parseDeploymentTemplate(strimziClient, template.getDeployment());
                ModelUtils.parsePodTemplate(strimziClient, template.getPod());
                strimziClient.templatePodLabels = Util.mergeLabelsOrAnnotations(strimziClient.templatePodLabels, DEFAULT_POD_LABELS);
            }

            strimziClient.version = versions.supportedVersion(kafkaAssembly.getSpec().getKafka().getVersion()).version();
            strimziClient.templatePersistentVolumeClaims = kafkaAssembly.getSpec().getKafka().getTemplate().getPersistentVolumeClaim();


            return strimziClient;
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        return portList;
    }

    /**
     * Generates Strimzi Client Deployment
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
                securityProvider.strimziClientPodSecurityContext(new PodSecurityProviderContextImpl(templateSecurityContext))
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
                        VolumeUtils.createVolumeMount(STRIMZI_CLIENT_DATA_VOLUME_NAME, STRIMZI_CLIENT_DATA_VOLUME_MOUNT),
                        VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME_NAME, CLUSTER_CA_CERTS_VOLUME_MOUNT))
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(securityProvider.strimziClientContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainerSecurityContext)))
                .build();

        containers.add(container);

        return containers;
    }

    /**
     * Generate the persistent volume claims for the storage It's called recursively on the related inner volumes if the
     * storage is of {@link Storage#TYPE_JBOD} type.
     * @return The PersistentVolumeClaims.
     */
    public List<PersistentVolumeClaim> generatePersistentVolumeClaims() {
        return PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(componentName, namespace, replicas, pvcstorage, false, labels, ownerReference, templatePersistentVolumeClaims, templateStatefulSetLabels);
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_STRIMZI_CLIENT_KAFKA_VERSION, version));
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
        if (pvcstorage != null) {
            if (pvcstorage instanceof PersistentClaimStorage persistentStorage) {
                String pvcBaseName = VolumeUtils.createVolumePrefix(persistentStorage.getId(), false) + "-" + componentName;
                volumeList.add(VolumeUtils.createPvcVolume(STRIMZI_CLIENT_DATA_VOLUME_NAME, pvcBaseName + "-0"));
            } else if (pvcstorage instanceof JbodStorage jbodStorage) {
                volumeList.add(VolumeUtils.createPvcVolume(STRIMZI_CLIENT_DATA_VOLUME_NAME, componentName));
            }
        }
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
