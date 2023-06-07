/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetUpdateStrategyBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverride;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * AbstractModel an abstract base model for all components of the {@code Kafka} custom resource
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public abstract class AbstractModel {

    public static final String STRIMZI_CLUSTER_OPERATOR_NAME = "strimzi-cluster-operator";

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractModel.class.getName());
    protected static final String LOG4J2_MONITOR_INTERVAL = "30";

    protected static final String DEFAULT_JVM_XMS = "128M";
    protected static final boolean DEFAULT_JVM_GC_LOGGING_ENABLED = false;

    /**
     * Init container related configuration
     */
    protected static final String INIT_NAME = "kafka-init";
    protected static final String INIT_VOLUME_NAME = "rack-volume";
    protected static final String INIT_VOLUME_MOUNT = "/opt/kafka/init";
    protected static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    protected static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";

    private static final Long DEFAULT_FS_GROUPID = 0L;

    public static final String ANCILLARY_CM_KEY_METRICS = "metrics-config.json";
    public static final String ANCILLARY_CM_KEY_LOG_CONFIG = "log4j.properties";

    public static final String DEFAULT_SECRET_DATA = "qingcloud";

    public static final String ENV_VAR_DYNAMIC_HEAP_PERCENTAGE = "STRIMZI_DYNAMIC_HEAP_PERCENTAGE";
    public static final String ENV_VAR_KAFKA_HEAP_OPTS = "KAFKA_HEAP_OPTS";
    public static final String ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS = "KAFKA_JVM_PERFORMANCE_OPTS";
    public static final String ENV_VAR_DYNAMIC_HEAP_MAX = "STRIMZI_DYNAMIC_HEAP_MAX";
    public static final String ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED = "STRIMZI_KAFKA_GC_LOG_ENABLED";
    public static final String ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES = "STRIMZI_JAVA_SYSTEM_PROPERTIES";
    public static final String ENV_VAR_STRIMZI_JAVA_OPTS = "STRIMZI_JAVA_OPTS";
    public static final String ENV_VAR_STRIMZI_GC_LOG_ENABLED = "STRIMZI_GC_LOG_ENABLED";

    /*
     * Default values for the Strimzi temporary directory
     */
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME = "strimzi-tmp";
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH = "/tmp";
    /*test*/ static final String STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE = "5Mi";

    /**
     * Annotation on PVCs storing the original configuration
     * Used to revert changes
     */
    public static final String ANNO_STRIMZI_IO_STORAGE = Annotations.STRIMZI_DOMAIN + "storage";
    public static final String ANNO_STRIMZI_IO_DELETE_CLAIM = Annotations.STRIMZI_DOMAIN + "delete-claim";

    /**
     * Configure statically defined environment variables which are passed to all operands.
     * This includes HTTP/HTTPS Proxy env vars or the FIPS_MODE.
     */
    protected static final List<EnvVar> STATIC_ENV_VARS;
    static {
        List<EnvVar> envVars = new ArrayList<>(3);

        if (System.getenv(ClusterOperatorConfig.HTTP_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.HTTP_PROXY, System.getenv(ClusterOperatorConfig.HTTP_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.HTTPS_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.HTTPS_PROXY, System.getenv(ClusterOperatorConfig.HTTPS_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.NO_PROXY) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.NO_PROXY, System.getenv(ClusterOperatorConfig.NO_PROXY)));
        }

        if (System.getenv(ClusterOperatorConfig.FIPS_MODE) != null)    {
            envVars.add(buildEnvVar(ClusterOperatorConfig.FIPS_MODE, System.getenv(ClusterOperatorConfig.FIPS_MODE)));
        }

        if (envVars.size() > 0) {
            STATIC_ENV_VARS = Collections.unmodifiableList(envVars);
        } else {
            STATIC_ENV_VARS = Collections.emptyList();
        }
    }

    protected final Reconciliation reconciliation;
    protected final String cluster;
    protected final String namespace;

    protected String name;
    protected String serviceName;
    protected String headlessServiceName;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    // Owner Reference information
    private String ownerApiVersion;
    private String ownerKind;
    private String ownerUid;

    protected Labels labels;

    /**
     * Application configuration
     */
    protected AbstractConfiguration configuration;
    private Logging logging;
    protected boolean gcLoggingEnabled = true;
    private JvmOptions jvmOptions;

    /**
     * Volume and Storage configuration
     */
    protected Storage storage;
    public static final String VOLUME_NAME = "data";
    public static final String KAFKA_MOUNT_PATH = "/var/lib/kafka";
    public static final String KAFKA_LOG_DIR = "kafka-log";
    protected String mountPath;

    /**
     * Metrics configuration
     */
    protected boolean isMetricsEnabled;
    protected static final String METRICS_PORT_NAME = "tcp-prometheus";
    protected static final int METRICS_PORT = 9404;
    protected MetricsConfig metricsConfigInCm;
    protected String ancillaryConfigMapName;
    protected String logAndMetricsConfigMountPath;
    protected String logAndMetricsConfigVolumeName;
    protected String logAndMetricsRackConfigMountPath;
    protected String logAndMetricsRackConfigVolumeName;

    /**
     * JMX configuration used for components such as Kafka and JMX Trans
     */
    protected static final String JMX_PORT_NAME = "jmx";
    protected static final int JMX_PORT = 9999;

    /**
     * Container configuration
     */
    private ResourceRequirements resources;
    protected io.strimzi.api.kafka.model.Probe startupProbeOptions;
    protected String readinessPath;
    protected io.strimzi.api.kafka.model.Probe readinessProbeOptions;
    protected String livenessPath;
    protected io.strimzi.api.kafka.model.Probe livenessProbeOptions;
    private Affinity userAffinity;
    private List<Toleration> tolerations;

    /**
     * Template configuration
     * Used to allow all components to have configurable labels, annotations, security context etc
     */
    protected Map<String, String> templateStatefulSetLabels;
    protected Map<String, String> templateStatefulSetAnnotations;
    protected Map<String, String> templatePodSetLabels;
    protected Map<String, String> templatePodSetAnnotations;
    protected Map<String, String> templateDeploymentLabels;
    protected Map<String, String> templateDeploymentAnnotations;
    protected io.strimzi.api.kafka.model.template.DeploymentStrategy templateDeploymentStrategy = io.strimzi.api.kafka.model.template.DeploymentStrategy.ROLLING_UPDATE;
    protected Map<String, String> templatePodLabels;
    protected Map<String, String> templatePodAnnotations;
    protected Map<String, String> templateServiceLabels;
    protected Map<String, String> templateServiceAnnotations;
    protected IpFamilyPolicy templateServiceIpFamilyPolicy;
    protected List<IpFamily> templateServiceIpFamilies;
    protected Map<String, String> templateHeadlessServiceLabels;
    protected Map<String, String> templateHeadlessServiceAnnotations;
    protected IpFamilyPolicy templateHeadlessServiceIpFamilyPolicy;
    protected List<IpFamily> templateHeadlessServiceIpFamilies;
    protected List<LocalObjectReference> templateImagePullSecrets;
    protected PodSecurityContext templateSecurityContext;
    protected int templateTerminationGracePeriodSeconds = 30;
    protected Map<String, String> templatePersistentVolumeClaimLabels;
    protected Map<String, String> templatePersistentVolumeClaimAnnotations;
    protected Map<String, String> templatePodDisruptionBudgetLabels;
    protected Map<String, String> templatePodDisruptionBudgetAnnotations;
    protected int templatePodDisruptionBudgetMaxUnavailable = 1;
    protected String templatePodPriorityClassName;
    protected String templatePodSchedulerName;
    protected List<HostAlias> templatePodHostAliases;
    protected List<TopologySpreadConstraint> templatePodTopologySpreadConstraints;
    protected PodManagementPolicy templatePodManagementPolicy = PodManagementPolicy.PARALLEL;
    protected Boolean templatePodEnableServiceLinks;
    protected Map<String, String> templateClusterRoleBindingLabels;
    protected Map<String, String> templateClusterRoleBindingAnnotations;
    protected Map<String, String> templateServiceAccountLabels;
    protected Map<String, String> templateServiceAccountAnnotations;
    protected Map<String, String> templateJmxSecretLabels;
    protected Map<String, String> templateJmxSecretAnnotations;
    protected String templateTmpDirSizeLimit;

    protected List<Condition> warningConditions = new ArrayList<>(0);

    /**
     * Constructor
     *
     * @param reconciliation   The reconciliation
     * @param resource         Kubernetes resource with metadata containing the namespace and cluster name
     * @param applicationName  Name of the application that the extending class is deploying
     */
    protected AbstractModel(Reconciliation reconciliation, HasMetadata resource, String applicationName) {
        this.reconciliation = reconciliation;
        this.cluster = resource.getMetadata().getName();
        this.namespace = resource.getMetadata().getNamespace();
        this.labels = Labels.generateDefaultLabels(resource, applicationName, STRIMZI_CLUSTER_OPERATOR_NAME);
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    protected void setImage(String image) {
        this.image = image;
    }

    protected void setReadinessProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.readinessProbeOptions = probe;
    }

    protected void setLivenessProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.livenessProbeOptions = probe;
    }

    protected void setStartupProbe(io.strimzi.api.kafka.model.Probe probe) {
        this.startupProbeOptions = probe;
    }

    /**
     * @return the default Kubernetes resource name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The Kubernetes service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @return The Kubernetes headless service name.
     */
    public String getHeadlessServiceName() {
        return headlessServiceName;
    }

    /**
     * @return The selector labels as an instance of the Labels object.
     */
    public Labels getSelectorLabels() {
        return getLabelsWithStrimziName(name, emptyMap()).strimziSelectorLabels();
    }

    /**
     * @param name the value for the {@code strimzi.io/name} key
     * @param additionalLabels a nullable map of additional labels to be added to this instance of Labels
     *
     * @return Labels object with the default labels merged with the provided additional labels and the new {@code strimzi.io/name} label
     */
    protected Labels getLabelsWithStrimziName(String name, Map<String, String> additionalLabels) {
        return labels.withStrimziName(name).withAdditionalLabels(additionalLabels);
    }

    /**
     * @param name the value for the {@code strimzi.io/name} key
     * @param podName the value for the {@code strimzi.io/pod-name} key
     * @param additionalLabels a nullable map of additional labels to be added to this instance of Labels
     *
     * @return Labels object with the default labels merged with the provided additional labels and the new {@code strimzi.io/name} label
     */
    protected Labels getLabelsWithStrimziNameAndPodName(String name, String podName, Map<String, String> additionalLabels) {
        return labels.withStrimziName(name).withStrimziPodName(podName).withAdditionalLabels(additionalLabels);
    }

    /**
     * @param name the value for the {@code strimzi.io/name} key
     * @param additionalLabels a nullable map of additional labels to be added to this instance of Labels
     *
     * @return Labels object with the default labels merged with the provided additional labels, the new {@code strimzi.io/name} label
     * and {@code strimzi.io/discovery} set to true to make the service discoverable
     */
    protected Labels getLabelsWithStrimziNameAndDiscovery(String name, Map<String, String> additionalLabels) {
        return getLabelsWithStrimziName(name, additionalLabels).withStrimziDiscovery();
    }

    /**
     * @return Whether metrics are enabled.
     */
    public boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    protected void setMetricsEnabled(boolean isMetricsEnabled) {
        this.isMetricsEnabled = isMetricsEnabled;
    }

    protected void setGcLoggingEnabled(boolean gcLoggingEnabled) {
        this.gcLoggingEnabled = gcLoggingEnabled;
    }

    protected abstract String getDefaultLogConfigFileName();

    /**
     * @return OrderedProperties map with all available loggers for current pod and default values.
     */
    public OrderedProperties getDefaultLogConfig() {
        String logConfigFileName = getDefaultLogConfigFileName();
        if (logConfigFileName == null || logConfigFileName.isEmpty()) {
            return new OrderedProperties();
        }
        return getOrderedProperties(reconciliation, getDefaultLogConfigFileName());
    }

    /**
     * Read a config file and returns the properties in a deterministic order.
     *
     * @param reconciliation The reconciliation
     * @param configFileName The filename.
     * @return The OrderedProperties of the inputted file.
     */
    public static OrderedProperties getOrderedProperties(Reconciliation reconciliation, String configFileName) {
        if (configFileName == null || configFileName.isEmpty()) {
            throw new IllegalArgumentException("configFileName must be non-empty string");
        }
        OrderedProperties properties = new OrderedProperties();
        InputStream is = AbstractModel.class.getResourceAsStream("/" + configFileName);
        if (is == null) {
            LOGGER.warnCr(reconciliation, "Cannot find resource '{}'", configFileName);
        } else {
            try {
                properties.addStringPairs(is);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Unable to read default log config from '{}'", configFileName);
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    LOGGER.errorCr(reconciliation, "Failed to close stream. Reason: " + e.getMessage());
                }
            }
        }
        return properties;
    }

    /**
     * Transforms map to log4j properties file format.
     *
     * @param properties map of log4j properties.
     * @return log4j properties as a String.
     */
    public String createLog4jProperties(OrderedProperties properties) {
        return properties.asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");
    }

    /**
     * @return The logging.
     */
    public Logging getLogging() {
        return logging;
    }

    protected void setLogging(Logging logging) {
        this.logging = logging;
    }


    /**
     * Regarding to used implementation we may need to patch an appender.
     * If the user does not provide the appender in tuple logger: level, it should be added and warn message printed.
     * @return true if patching needs to be done due to dynamic configuration, otherwise false
     */
    protected boolean shouldPatchLoggerAppender() {
        return false;
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param logging       The logging configuration from the custom resource
     * @param externalCm    The user-provided ConfigMap with custom Log4j / Log4j2 file
     *
     * @return              String with the Log4j / Log4j2 properties used for configuration
     */
    public String loggingConfiguration(Logging logging, ConfigMap externalCm) {
        if (logging instanceof InlineLogging) {
            InlineLogging inlineLogging = (InlineLogging) logging;
            OrderedProperties newSettings = getDefaultLogConfig();

            if (inlineLogging.getLoggers() != null) {
                // Inline logging as specified and some loggers are configured
                if (shouldPatchLoggerAppender()) {
                    String rootAppenderName = getRootAppenderNamesFromDefaultLoggingConfig(newSettings);
                    String newRootLogger = inlineLogging.getLoggers().get("log4j.rootLogger");
                    newSettings.addMapPairs(inlineLogging.getLoggers());

                    if (newRootLogger != null && !rootAppenderName.isEmpty() && !newRootLogger.contains(",")) {
                        // this should never happen as appender name is added in default configuration
                        LOGGER.debugCr(reconciliation, "Newly set rootLogger does not contain appender. Setting appender to {}.", rootAppenderName);
                        String level = newSettings.asMap().get("log4j.rootLogger");
                        newSettings.addPair("log4j.rootLogger", level + ", " + rootAppenderName);
                    }
                } else {
                    newSettings.addMapPairs(inlineLogging.getLoggers());
                }
            }

            return createLog4jProperties(newSettings);
        } else if (logging instanceof ExternalLogging) {
            ExternalLogging externalLogging = (ExternalLogging) logging;
            if (externalLogging.getValueFrom() != null && externalLogging.getValueFrom().getConfigMapKeyRef() != null && externalLogging.getValueFrom().getConfigMapKeyRef().getKey() != null) {
                if (externalCm != null && externalCm.getData() != null && externalCm.getData().containsKey(externalLogging.getValueFrom().getConfigMapKeyRef().getKey())) {
                    return maybeAddMonitorIntervalToExternalLogging(externalCm.getData().get(externalLogging.getValueFrom().getConfigMapKeyRef().getKey()));
                } else {
                    throw new InvalidResourceException(
                        String.format("ConfigMap %s with external logging configuration does not exist or doesn't contain the configuration under the %s key.",
                            externalLogging.getValueFrom().getConfigMapKeyRef().getName(),
                            externalLogging.getValueFrom().getConfigMapKeyRef().getKey())
                    );
                }
            } else {
                throw new InvalidResourceException("Property logging.valueFrom has to be specified when using external logging.");
            }
        } else {
            LOGGER.debugCr(reconciliation, "logging is not set, using default loggers");
            return createLog4jProperties(getDefaultLogConfig());
        }
    }

    private String getRootAppenderNamesFromDefaultLoggingConfig(OrderedProperties newSettings) {
        String logger = newSettings.asMap().get("log4j.rootLogger");
        String appenderName = "";
        if (logger != null) {
            String[] tmp = logger.trim().split(",", 2);
            if (tmp.length == 2) {
                appenderName = tmp[1].trim();
            } else {
                LOGGER.warnCr(reconciliation, "Logging configuration for root logger does not contain appender.");
            }
        } else {
            LOGGER.warnCr(reconciliation, "Logger log4j.rootLogger not set.");
        }
        return appenderName;
    }

    /**
     * Adds 'monitorInterval=30' to external logging ConfigMap. If ConfigMap already has this value, it is persisted.
     *
     * @param data String with log4j2 properties in format key=value separated by new lines
     * @return log4j2 configuration with monitorInterval property
     */
    protected String maybeAddMonitorIntervalToExternalLogging(String data) {
        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(data);

        Optional<String> mi = orderedProperties.asMap().keySet().stream()
                .filter(key -> key.matches("^monitorInterval$")).findFirst();
        if (mi.isPresent()) {
            return data;
        } else {
            // do not override custom value
            return data + "\nmonitorInterval=" + LOG4J2_MONITOR_INTERVAL + "\n";
        }
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults. This is used with most operands, but
     * not all of them. Kafka brokers have own methods in the KafkaCluster class. So does the Bridge. And Kafka Exporter
     * has no metrics or logging ConfigMap at all.
     *
     * @param metricsAndLogging The external CMs
     * @return The generated ConfigMap.
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        Map<String, String> data = new HashMap<>(2);
        data.put(getAncillaryConfigMapKeyLogConfig(), loggingConfiguration(getLogging(), metricsAndLogging.getLoggingCm()));
        if (getMetricsConfigInCm() != null) {
            String parseResult = metricsConfiguration(metricsAndLogging.getMetricsCm());
            if (parseResult != null) {
                this.setMetricsEnabled(true);
                data.put(ANCILLARY_CM_KEY_METRICS, parseResult);
            }
        }
        return createConfigMap(ancillaryConfigMapName, data);
    }

    /**
     * Generates Prometheus metrics configuration based on the JMXExporter configuration from the user-provided ConfigMap.
     *
     * @param externalCm    ConfigMap with the JMX Prometheus configuration YAML
     *
     * @return              String with JSON formatted metrics configuration
     */
    public String metricsConfiguration(ConfigMap externalCm) {
        if (getMetricsConfigInCm() != null) {
            if (getMetricsConfigInCm() instanceof JmxPrometheusExporterMetrics) {
                if (externalCm == null) {
                    LOGGER.warnCr(reconciliation, "ConfigMap {} does not exist. Metrics disabled.",
                            ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName());
                    throw new InvalidResourceException("ConfigMap " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName() + " does not exist.");
                } else {
                    String data = externalCm.getData().get(((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey());
                    if (data == null) {
                        LOGGER.warnCr(reconciliation, "ConfigMap {} does not contain specified key {}.", ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName(),
                                ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey());
                        throw new InvalidResourceException("ConfigMap " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getName()
                                + " does not contain specified key " + ((JmxPrometheusExporterMetrics) getMetricsConfigInCm()).getValueFrom().getConfigMapKeyRef().getKey() + ".");
                    } else {
                        if (data.isEmpty()) {
                            return "{}";
                        }
                        try {
                            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
                            Object yaml = yamlReader.readValue(data, Object.class);
                            ObjectMapper jsonWriter = new ObjectMapper();
                            return jsonWriter.writeValueAsString(yaml);
                        } catch (JsonProcessingException e) {
                            throw new InvalidResourceException("Parsing metrics configuration failed. ", e);
                        }
                    }
                }
            } else {
                LOGGER.warnCr(reconciliation, "Unknown type of metrics {}.", getMetricsConfigInCm().getClass());
                throw new InvalidResourceException("Unknown type of metrics " + getMetricsConfigInCm().getClass() + ".");
            }
        }
        return null;
    }

    protected void setMetricsConfigInCm(MetricsConfig metricsConfigInCm) {
        this.metricsConfigInCm = metricsConfigInCm;
    }

    public MetricsConfig getMetricsConfigInCm() {
        return metricsConfigInCm;
    }

    /**
     * Returns name of config map used for storing metrics and logging configuration.
     * @return The name of config map used for storing metrics and logging configuration.
     */
    public String getAncillaryConfigMapName() {
        return ancillaryConfigMapName;
    }

    /**
     * Returns a lit of environment variables which are required by all containers.
     *
     * Contains:
     * The mirrored HTTP Proxy environment variables
     *
     * @return  List of required environment variables for all containers
     */
    protected List<EnvVar> getRequiredEnvVars() {
        // HTTP Proxy configuration should be passed to all images
        return STATIC_ENV_VARS;
    }

    /**
     * To be overridden by implementing classes
     *
     * @return null
     */
    protected List<EnvVar> getEnvVars() {
        return null;
    }

    /**
     * @return The storage.
     */
    public Storage getStorage() {
        return storage;
    }

    /**
     * Set the Storage
     *
     * @param storage Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        validatePersistentStorage(storage);
        this.storage = storage;
    }

    /**
     * Validates persistent storage
     * If storage is of a persistent type, validations are made
     * If storage is not of a persistent type, validation passes
     *
     * @param storage   Persistent Storage configuration
     * @throws InvalidResourceException if validations fails for any reason
     */
    protected static void validatePersistentStorage(Storage storage)   {
        if (storage instanceof PersistentClaimStorage) {
            PersistentClaimStorage persistentClaimStorage = (PersistentClaimStorage) storage;
            checkPersistentStorageSizeIsValid(persistentClaimStorage);

        } else if (storage instanceof JbodStorage)  {
            JbodStorage jbodStorage = (JbodStorage) storage;

            if (jbodStorage.getVolumes().size() == 0)   {
                throw new InvalidResourceException("JbodStorage needs to contain at least one volume!");
            }

            for (Storage jbodVolume : jbodStorage.getVolumes()) {
                if (jbodVolume instanceof PersistentClaimStorage) {
                    PersistentClaimStorage persistentClaimStorage = (PersistentClaimStorage) jbodVolume;
                    checkPersistentStorageSizeIsValid(persistentClaimStorage);
                }
            }
        }
    }

    /**
     * Checks if the supplied PersistentClaimStorage has a valid size
     *
     * @param storage   PersistentClaimStorage configuration
     *
     * @throws InvalidResourceException if the persistent storage size is not valid
     */
    private static void checkPersistentStorageSizeIsValid(PersistentClaimStorage storage)   {
        if (storage.getSize() == null || storage.getSize().isEmpty()) {
            throw new InvalidResourceException("The size is mandatory for a persistent-claim storage");
        }
    }

    /**
     * @return an implementation of AbstractConfiguration configured by a user for a component.
     */
    public AbstractConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Set the configuration object which may be configured by the user for some components.
     *
     * @param configuration Configuration settings for a component.
     */
    protected void setConfiguration(AbstractConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * @return The image name.
     */
    public String getImage() {
        return image;
    }

    /**
     * @return the name of the service account used by the deployed cluster for Kubernetes API operations.
     */
    protected String getServiceAccountName() {
        return null;
    }

    /**
     * @return the name of the role used by the service account for the deployed cluster for Kubernetes API operations.
     */
    protected String getRoleName() {
        throw new RuntimeException("Unsupported method: this method should be overridden");
    }

    /**
     * @return the cluster name.
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Gets the name of a given pod in a StatefulSet.
     *
     * @param podId The Id (ordinal) of the pod.
     * @return The name of the pod with the given name.
     */
    public String getPodName(int podId) {
        return name + "-" + podId;
    }

    /**
     * Sets the affinity as configured by the user in the cluster CR.
     *
     * @param affinity  Affinity configured by the user
     */
    protected void setUserAffinity(Affinity affinity) {
        this.userAffinity = affinity;
    }

    /**
     * Gets the affinity as configured by the user in the cluster CR.
     */
    protected Affinity getUserAffinity() {
        return this.userAffinity;
    }

    /**
     * Gets the tolerations as configured by the user in the cluster CR.
     *
     * @return The tolerations.
     */
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    /**
     * Sets the tolerations as configured by the user in the cluster CR.
     *
     * @param tolerations The tolerations.
     */
    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    /**
     * Gets the affinity to use in a Pod template (nested in a StatefulSet, or Deployment).
     * In general this may include extra rules than just the {@link #userAffinity}.
     *
     * By default it is just the {@link #userAffinity}.
     */
    protected Affinity getMergedAffinity() {
        return getUserAffinity();
    }

    /**
     * Default null, to be overridden by implementing classes
     *
     * @return a list of init containers to add to the StatefulSet/Deployment
     */
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {

        return null;
    }

    /**
     * To be overridden by implementing classes
     *
     * @return a list of containers to add to the StatefulSet/Deployment
     */
    protected abstract List<Container> getContainers(ImagePullPolicy imagePullPolicy);

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        LOGGER.traceCr(reconciliation, "Created container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = createServicePort(name, port, targetPort, null, protocol);
        LOGGER.traceCr(reconciliation, "Created service port {}", servicePort);
        return servicePort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, Integer nodePort, String protocol) {
        ServicePortBuilder builder = new ServicePortBuilder()
            .withName(name)
            .withProtocol(protocol)
            .withPort(port)
            .withNewTargetPort(targetPort);
        if (nodePort != null) {
            builder.withNodePort(nodePort);
        }
        ServicePort servicePort = builder.build();
        LOGGER.traceCr(reconciliation, "Created service port {}", servicePort);
        return servicePort;
    }

    /**
     * Creates list of PersistentVolumeClaims required by stateful deployments (Kafka and Zoo). This method calls itself
     * recursively to handle volumes inside JBOD storage. When it calls itself to handle the volumes inside JBOD array,
     * the {@code jbod} flag should be set to {@code true}. When called from outside, it should be set to {@code false}.
     *
     * @param storage   The storage configuration
     * @param jbod      Indicator whether the {@code storage} is part of JBOD array or not
     *
     * @return          List with Persistent Volume Claims
     */
    protected List<PersistentVolumeClaim> createPersistentVolumeClaims(Storage storage, boolean jbod)   {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof PersistentClaimStorage) {
                PersistentClaimStorage persistentStorage = (PersistentClaimStorage) storage;
                String pvcBaseName = VolumeUtils.createVolumePrefix(persistentStorage.getId(), jbod) + "-" + name;

                for (int i = 0; i < replicas; i++) {
                    pvcs.add(createPersistentVolumeClaim(i, pvcBaseName + "-" + i, persistentStorage));
                }
            } else if (storage instanceof JbodStorage) {
                for (SingleVolumeStorage volume : ((JbodStorage) storage).getVolumes()) {
                    // it's called recursively for setting the information from the current volume
                    pvcs.addAll(createPersistentVolumeClaims(volume, true));
                }
            }
        }

        return pvcs;
    }

    /**
     * createPersistentVolumeClaim is called uniquely for each ordinal (Broker ID) of a stateful set
     *
     * @param ordinalId the ordinal of the pod/broker for which the persistent volume claim is being created
     *                  used to retrieve the optional broker storage overrides for each broker
     * @param name      the name of the persistent volume claim to be created
     * @param storage   the user supplied configuration of the PersistentClaimStorage
     *
     * @return PersistentVolumeClaim
     */
    protected PersistentVolumeClaim createPersistentVolumeClaim(int ordinalId, String name, PersistentClaimStorage storage) {
        Map<String, Quantity> requests = new HashMap<>(1);
        requests.put("storage", new Quantity(storage.getSize(), null));

        LabelSelector selector = null;
        if (storage.getSelector() != null && !storage.getSelector().isEmpty()) {
            selector = new LabelSelector(null, storage.getSelector());
        }

        String storageClass = storage.getStorageClass();
        if (storage.getOverrides() != null) {
            storageClass = storage.getOverrides().stream()
                    .filter(broker -> broker != null
                            && broker.getBroker() != null
                            && broker.getBroker() == ordinalId
                            && broker.getStorageClass() != null)
                    .map(PersistentClaimStorageOverride::getStorageClass)
                    .findAny()
                    // if none are found for broker do not change storage class from overrides
                    .orElse(storageClass);
        }

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    // labels with the Strimzi name label of the component (this.name)
                    .withLabels(getLabelsWithStrimziName(this.name, templatePersistentVolumeClaimLabels).toMap())
                    .withAnnotations(Util.mergeLabelsOrAnnotations(Collections.singletonMap(ANNO_STRIMZI_IO_DELETE_CLAIM, Boolean.toString(storage.isDeleteClaim())), templatePersistentVolumeClaimAnnotations))
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(requests)
                    .endResources()
                    .withStorageClassName(storageClass)
                    .withSelector(selector)
                    .withVolumeMode("Filesystem")
                .endSpec()
                .build();

        // if the persistent volume claim has to be deleted when the cluster is un-deployed then set an owner reference of the CR
        if (storage.isDeleteClaim())    {
            pvc.getMetadata().setOwnerReferences(Collections.singletonList(createOwnerReference()));
        }

        return pvc;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {
        return createConfigMap(name, labels.toMap(), data);
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> labels, Map<String, String> data) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withData(data)
                .build();
    }

    protected Secret createSecret(String name, Map<String, String> data, Map<String, String> customAnnotations) {
        return ModelUtils.createSecret(name, namespace, labels, createOwnerReference(), data, customAnnotations, emptyMap());
    }

    protected Secret createJmxSecret(String name, Map<String, String> data) {
        return ModelUtils.createSecret(name, namespace, labels, createOwnerReference(), data, templateJmxSecretAnnotations, templateJmxSecretLabels);
    }

    protected Service createService(String type, List<ServicePort> ports, Map<String, String> annotations) {
        return createService(serviceName, type, ports, getLabelsWithStrimziName(serviceName, templateServiceLabels),
                getSelectorLabels(), annotations, templateServiceIpFamilyPolicy, templateServiceIpFamilies);
    }

    protected Service createDiscoverableService(String type, List<ServicePort> ports, Map<String, String> labels, Map<String, String> annotations) {
        return createService(serviceName, type, ports, getLabelsWithStrimziNameAndDiscovery(name, labels), getSelectorLabels(), annotations, templateServiceIpFamilyPolicy, templateServiceIpFamilies);
    }

    protected Service createService(String name, String type, List<ServicePort> ports, Labels labels, Labels selector, Map<String, String> annotations, IpFamilyPolicy ipFamilyPolicy, List<IpFamily> ipFamilies) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.toMap())
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(selector.toMap())
                    .withPorts(ports)
                .endSpec()
                .build();

        if (ipFamilyPolicy != null) {
            service.getSpec().setIpFamilyPolicy(ipFamilyPolicy.toValue());
        }

        if (ipFamilies != null && !ipFamilies.isEmpty()) {
            service.getSpec().setIpFamilies(ipFamilies.stream().map(IpFamily::toValue).collect(Collectors.toList()));
        }

        LOGGER.traceCr(reconciliation, "Created service {}", service);
        return service;
    }

    /**
     * Creates a headless service
     *
     * Uses Alpha annotation service.alpha.kubernetes.io/tolerate-unready-endpoints for older versions of Kubernetes still supported by Strimzi,
     * replaced by the publishNotReadyAddresses field in the spec,  annotation is ignored in later versions of Kubernetes
     */
    protected Service createHeadlessService(List<ServicePort> ports) {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(headlessServiceName)
                    .withLabels(getLabelsWithStrimziName(name, templateHeadlessServiceLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(annotations, templateHeadlessServiceAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(getSelectorLabels().toMap())
                    .withPorts(ports)
                    .withPublishNotReadyAddresses(true)
                .endSpec()
                .build();

        if (templateHeadlessServiceIpFamilyPolicy != null) {
            service.getSpec().setIpFamilyPolicy(templateHeadlessServiceIpFamilyPolicy.toValue());
        }

        if (templateHeadlessServiceIpFamilies != null && !templateHeadlessServiceIpFamilies.isEmpty()) {
            service.getSpec().setIpFamilies(templateHeadlessServiceIpFamilies.stream().map(IpFamily::toValue).collect(Collectors.toList()));
        }

        LOGGER.traceCr(reconciliation, "Created headless service {}", service);
        return service;
    }

    protected StatefulSet createStatefulSet(
            Map<String, String> stsAnnotations,
            Map<String, String> podAnnotations,
            List<Volume> volumes,
            List<PersistentVolumeClaim> volumeClaims,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<LocalObjectReference> imagePullSecrets,
            boolean isOpenShift) {

        PodSecurityContext securityContext = templateSecurityContext;

        // if a persistent volume claim is requested and the running cluster is a Kubernetes one (non-openshift) and we
        // have no user configured PodSecurityContext we set the podSecurityContext.
        // This is to give each pod write permissions under a specific group so that if a pod changes users it does not have permission issues.
        if (ModelUtils.containsPersistentStorage(storage) && !isOpenShift && securityContext == null) {
            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractModel.DEFAULT_FS_GROUPID)
                    .build();
        }

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templateStatefulSetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(stsAnnotations, templateStatefulSetAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy(templatePodManagementPolicy.toValue())
                    .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                    .withServiceName(headlessServiceName)
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(name)
                            .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                            .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                        .endMetadata()
                        .withNewSpec()
                            .withServiceAccountName(getServiceAccountName())
                            .withEnableServiceLinks(templatePodEnableServiceLinks)
                            .withAffinity(affinity)
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                            .withSecurityContext(securityContext)
                            .withPriorityClassName(templatePodPriorityClassName)
                            .withSchedulerName(templatePodSchedulerName != null ? templatePodSchedulerName : "default-scheduler")
                            .withHostAliases(templatePodHostAliases)
                            .withTopologySpreadConstraints(templatePodTopologySpreadConstraints)
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(volumeClaims)
                .endSpec()
                .build();

        return statefulSet;
    }

    /**
     * Creates the StrimziPodSet with the Pods which currently correspond to the existing StatefulSet pods.
     *
     * @param replicas                  Defines how many pods should be generated and stored in this StrimziPodSet
     * @param setAnnotations            Map with annotations which should be set on the StrimziPodSet
     * @param podAnnotationsProvider    Function which provides annotation map for a specific Pod. A function is used
     *                                  instead of providing a map because in some cases, each pod might have different
     *                                  annotations. So they need to be generated per-pod.
     * @param volumes                   Function which returns a list of volumes which should be used by the Pod and its containers
     * @param affinity                  Affinity rules for the pods
     * @param initContainers            List of init containers which should be used in the pods
     * @param containers                List of containers which should be used in the pods
     * @param imagePullSecrets          List of image pull secrets with container registry credentials
     * @param isOpenShift               Flag to specify whether we are on OpenShift or not
     *
     * @return                          Generated StrimziPodSet with all pods
     */
    protected StrimziPodSet createPodSet(
            int replicas,
            Map<String, String> setAnnotations,
            Function<Integer, Map<String, String>> podAnnotationsProvider,
            Function<String, List<Volume>> volumes,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<LocalObjectReference> imagePullSecrets,
            boolean isOpenShift)  {
        List<Map<String, Object>> pods = new ArrayList<>(replicas);

        for (int i = 0; i < replicas; i++)  {
            String podName = getPodName(i);
            Pod pod = createStatefulPod(
                    name,
                    podName,
                    podAnnotationsProvider.apply(i),
                    volumes.apply(podName),
                    affinity,
                    initContainers,
                    containers,
                    imagePullSecrets,
                    isOpenShift);

            pods.add(PodSetUtils.podToMap(pod));
        }

        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodSetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(setAnnotations, templatePodSetAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                    .addAllToPods(pods)
                .endSpec()
                .build();
    }

    /**
     * Generates a stateful Pod. Unlike regular pods or pod templates, it uses some additional fields to configure
     * things such as subdomain etc. which are normally generated by StatefulSets.
     *
     * @param strimziPodSetName Name of the StrimziPodSet which will control this pod. This is used for labeling
     * @param podName           Name of the pod
     * @param podAnnotations    Map with annotation which should be set on the Pods
     * @param volumes           Function which returns a list of volumes which should be used by the Pod and its containers
     * @param affinity          Affinity rules for the pods
     * @param initContainers    List of init containers which should be used in the pods
     * @param containers        List of containers which should be used in the pods
     * @param imagePullSecrets  List of image pull secrets with container registry credentials
     * @param isOpenShift       Flag to specify whether we are on OpenShift or not
     *
     * @return                  Generated pod for a use within StrimziPodSet
     */
    protected Pod createStatefulPod(
            String strimziPodSetName,
            String podName,
            Map<String, String> podAnnotations,
            List<Volume> volumes,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<LocalObjectReference> imagePullSecrets,
            boolean isOpenShift) {

        PodSecurityContext securityContext = templateSecurityContext;

        // if a persistent volume claim is requested and the running cluster is a Kubernetes one (non-openshift) and we
        // have no user configured PodSecurityContext we set the podSecurityContext.
        // This is to give each pod write permissions under a specific group so that if a pod changes users it does not have permission issues.
        if (ModelUtils.containsPersistentStorage(storage) && !isOpenShift && securityContext == null) {
            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractModel.DEFAULT_FS_GROUPID)
                    .build();
        }

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(podName)
                    .withLabels(getLabelsWithStrimziNameAndPodName(name, podName, templatePodLabels).withStatefulSetPod(podName).withStrimziPodSetController(strimziPodSetName).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                .endMetadata()
                .withNewSpec()
                    .withRestartPolicy("Always")
                    .withHostname(podName)
                    .withSubdomain(headlessServiceName)
                    .withServiceAccountName(getServiceAccountName())
                    .withEnableServiceLinks(templatePodEnableServiceLinks)
                    .withAffinity(affinity)
                    .withInitContainers(initContainers)
                    .withContainers(containers)
                    .withVolumes(volumes)
                    .withTolerations(getTolerations())
                    .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                    .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                    .withSecurityContext(securityContext)
                    .withPriorityClassName(templatePodPriorityClassName)
                    .withSchedulerName(templatePodSchedulerName != null ? templatePodSchedulerName : "default-scheduler")
                    .withHostAliases(templatePodHostAliases)
                    .withTopologySpreadConstraints(templatePodTopologySpreadConstraints)
                .endSpec()
                .build();

        // Set the pod revision annotation
        pod.getMetadata().getAnnotations().put(PodRevision.STRIMZI_REVISION_ANNOTATION, PodRevision.getRevision(reconciliation, pod));

        return pod;
    }

    protected Pod createPod(
            String name,
            Map<String, String> podAnnotations,
            List<Volume> volumes,
            List<Container> initContainers,
            List<Container> containers,
            List<LocalObjectReference> imagePullSecrets,
            boolean isOpenShift) {

        PodSecurityContext securityContext = templateSecurityContext;

        // if a persistent volume claim is requested and the running cluster is a Kubernetes one (non-openshift) and we
        // have no user configured PodSecurityContext we set the podSecurityContext.
        // This is to give each pod write permissions under a specific group so that if a pod changes users it does not have permission issues.
        if (ModelUtils.containsPersistentStorage(storage) && !isOpenShift && securityContext == null) {
            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractModel.DEFAULT_FS_GROUPID)
                    .build();
        }

        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withRestartPolicy("Never")
                    .withServiceAccountName(getServiceAccountName())
                    .withEnableServiceLinks(templatePodEnableServiceLinks)
                    .withAffinity(getUserAffinity())
                    .withInitContainers(initContainers)
                    .withContainers(containers)
                    .withVolumes(volumes)
                    .withTolerations(getTolerations())
                    .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                    .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                    .withSecurityContext(securityContext)
                    .withPriorityClassName(templatePodPriorityClassName)
                    .withSchedulerName(templatePodSchedulerName != null ? templatePodSchedulerName : "default-scheduler")
                .endSpec()
                .build();

        return pod;
    }

    protected Deployment createDeployment(
            DeploymentStrategy updateStrategy,
            Map<String, String> deploymentAnnotations,
            Map<String, String> podAnnotations,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes,
            List<LocalObjectReference> imagePullSecrets) {

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templateDeploymentLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(deploymentAnnotations, templateDeploymentAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withStrategy(updateStrategy)
                    .withReplicas(replicas)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                            .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, templatePodAnnotations))
                        .endMetadata()
                        .withNewSpec()
                            .withAffinity(affinity)
                            .withServiceAccountName(getServiceAccountName())
                            .withEnableServiceLinks(templatePodEnableServiceLinks)
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                            .withSecurityContext(templateSecurityContext)
                            .withPriorityClassName(templatePodPriorityClassName)
                            .withSchedulerName(templatePodSchedulerName)
                            .withHostAliases(templatePodHostAliases)
                            .withTopologySpreadConstraints(templatePodTopologySpreadConstraints)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return dep;
    }

    /**
     * Build an environment variable with the provided name and value
     *
     * @param name The name of the environment variable
     * @param value The value of the environment variable
     * @return The environment variable object
     */
    protected static EnvVar buildEnvVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    /**
     * Build an environment variable which will use a value from a secret
     *
     * @param name The name of the environment variable
     * @param secret The name of the secret where the value is stored
     * @param key The key under which the value is stored in the secret
     *
     * @return The environment variable object
     */
    protected static EnvVar buildEnvVarFromSecret(String name, String secret, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(secret)
                        .withKey(key)
                    .endSecretKeyRef()
                .endValueFrom()
                .build();
    }

    /**
     * Build an environment variable instance with the provided name from a field reference
     * using the Downward API
     *
     * @param name The name of the environment variable
     * @param field The field path from which the value is set
     *
     * @return The environment variable object
     */
    protected static EnvVar buildEnvVarFromFieldRef(String name, String field) {

        EnvVarSource envVarSource = new EnvVarSourceBuilder()
                .withNewFieldRef()
                    .withFieldPath(field)
                .endFieldRef()
                .build();

        return new EnvVarBuilder()
                .withName(name)
                .withValueFrom(envVarSource)
                .build();
    }

    /**
     * Gets the given container's environment as a Map
     *
     * @param container The container to retrieve the EnvVars from
     *
     * @return A map of the environment variables of the given container
     *         The Environmental variable values indexed by their names
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
            Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                // On duplicates, last-in wins
                (u, v) -> v));
    }

    /**
     * @return The Labels object.
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * @param labels The Labels object.
     */
    public void setLabels(Labels labels) {
        this.labels = labels;
    }

    /**
     * @param resources The resource requirements.
     */
    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    /**
     * @return The resource requirements.
     */
    public ResourceRequirements getResources() {
        return resources;
    }

    /**
     * @param jvmOptions The JVM options.
     */
    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    /**
     * @return The JVM options.
     */
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    /**
     * Generate the OwnerReference object to link newly created objects to their parent (the custom resource)
     *
     * @return The OwnerReference object
     */
    protected OwnerReference createOwnerReference() {
        return new OwnerReferenceBuilder()
                .withApiVersion(ownerApiVersion)
                .withKind(ownerKind)
                .withName(cluster)
                .withUid(ownerUid)
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
    }

    /**
     * Set fields needed to generate the OwnerReference object
     *
     * @param parent The resource which should be used as parent. It will be used to gather the date needed for generating OwnerReferences.
     */
    protected void setOwnerReference(HasMetadata parent)  {
        this.ownerApiVersion = parent.getApiVersion();
        this.ownerKind = parent.getKind();
        this.ownerUid = parent.getMetadata().getUid();
    }

    /**
     * Creates the PodDisruptionBudget
     *
     * @return The default PodDisruptionBudget
     */
    protected PodDisruptionBudget createPodDisruptionBudget()   {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodDisruptionBudgetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(templatePodDisruptionBudgetAnnotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(templatePodDisruptionBudgetMaxUnavailable)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates the PodDisruptionBudgetV1Beta1
     *
     * @return The default PodDisruptionBudgetV1Beta1
     */
    protected io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget createPodDisruptionBudgetV1Beta1()   {
        return new io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodDisruptionBudgetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(templatePodDisruptionBudgetAnnotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(templatePodDisruptionBudgetMaxUnavailable)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates a PodDisruptionBudget for use with custom pod controller (such as StrimziPodSetController). Unlike
     * built-in controllers (Deployments, StatefulSets, Jobs, DaemonSets, ...), custom pod controllers can use only PDBs
     * with minAvailable in absolute numbers (i.e. no percentages).
     * See https://kubernetes.io/docs/tasks/run-application/configure-pdb/#arbitrary-controllers-and-selectors for more
     * details.
     *
     * @return The default PodDisruptionBudget
     */
    protected PodDisruptionBudget createCustomControllerPodDisruptionBudget()   {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodDisruptionBudgetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(templatePodDisruptionBudgetAnnotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withMinAvailable(new IntOrString(replicas - templatePodDisruptionBudgetMaxUnavailable))
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates a PodDisruptionBudget V1Beta1 for use with custom pod controller (such as StrimziPodSetController). Unlike
     * built-in controllers (Deployments, StatefulSets, Jobs, DaemonSets, ...), custom pod controllers can use only PDBs
     * with minAvailable in absolute numbers (i.e. no percentages).
     * See https://kubernetes.io/docs/tasks/run-application/configure-pdb/#arbitrary-controllers-and-selectors for more
     * details.
     *
     * @return The default PodDisruptionBudget V1Beta1
     */
    protected io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget createCustomControllerPodDisruptionBudgetV1Beta1()   {
        return new io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templatePodDisruptionBudgetLabels).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(templatePodDisruptionBudgetAnnotations)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withMinAvailable(new IntOrString(replicas - templatePodDisruptionBudgetMaxUnavailable))
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * When ImagePullPolicy is not specified by the user, Kubernetes will automatically set it based on the image
     *    :latest results in        Always
     *    anything else results in  IfNotPresent
     * This causes issues in diffing. To work around this we emulate here the Kubernetes defaults and set the policy accordingly on our side.
     *
     * This is applied to the Strimzi Kafka images which use the tag format :latest-kafka-x.y.z but have the same function
     * as if they were :latest
     * Therefore they should behave the same with an ImagePullPolicy of Always.
     *
     * @param requestedImagePullPolicy  The imagePullPolicy requested by the user (is always preferred when set, ignored when null)
     * @param image The image used for the container, from its tag we determine the default policy if requestedImagePullPolicy is null
     *
     * @return  The Image Pull Policy: Always, Never or IfNotPresent
     */
    protected String determineImagePullPolicy(ImagePullPolicy requestedImagePullPolicy, String image)  {
        if (requestedImagePullPolicy != null)   {
            return requestedImagePullPolicy.toString();
        }

        if (image.toLowerCase(Locale.ENGLISH).contains(":latest"))  {
            return ImagePullPolicy.ALWAYS.toString();
        } else {
            return ImagePullPolicy.IFNOTPRESENT.toString();
        }
    }

    public String getAncillaryConfigMapKeyLogConfig() {
        return ANCILLARY_CM_KEY_LOG_CONFIG;
    }

    /**
     * @param cluster The cluster name
     * @return The name of the Cluster CA certificate secret.
     */
    public static String clusterCaCertSecretName(String cluster)  {
        return KafkaResources.clusterCaCertificateSecretName(cluster);
    }

    /**
     * @param cluster The cluster name
     * @return The name of the Cluster CA key secret.
     */
    public static String clusterCaKeySecretName(String cluster)  {
        return KafkaResources.clusterCaKeySecretName(cluster);
    }

    /**
     * @return The service account.
     */
    public ServiceAccount generateServiceAccount() {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(getServiceAccountName())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(getLabelsWithStrimziName(name, templateServiceAccountLabels).toMap())
                    .withAnnotations(templateServiceAccountAnnotations)
                .endMetadata()
            .build();
    }

    /**
     * @param namespace The namespace the role will be deployed into
     * @param rules the list of rules associated with this role
     *
     * @return The role for the component.
     */
    public Role generateRole(String namespace, List<PolicyRule> rules) {
        return new RoleBuilder()
                .withNewMetadata()
                    .withName(getRoleName())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                    .addToLabels(labels.toMap())
                .endMetadata()
                .withRules(rules)
                .build();
    }

    /**
     * @param name The name of the rolebinding
     * @param namespace The namespace the rolebinding will be deployed into
     * @param roleRef a reference to a Role to bind to
     * @param subjects a list of subject ServiceAccounts to bind the role to
     *
     * @return The RoleBinding for the component with the given name and namespace.
     */
    public RoleBinding generateRoleBinding(String name, String namespace, RoleRef roleRef, List<Subject> subjects) {
        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(labels.toMap())
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(subjects)
                .build();
    }

    /**
     * Adds the supplied list of user configured container environment variables {@see io.strimzi.api.kafka.model.ContainerEnvVar} to the
     * supplied list of fabric8 environment variables {@see io.fabric8.kubernetes.api.model.EnvVar},
     * checking first if the environment variable key has already been set in the existing list and then converts them.
     *
     * If a key is already in use then the container environment variable will not be added to the environment variable
     * list and a warning will be logged.
     *
     * @param existingEnvs  The list of fabric8 environment variable object that will be added to.
     * @param containerEnvs The list of container environment variable objects to be converted and added to the existing
     *                      environment variable list
     **/
    protected void addContainerEnvsToExistingEnvs(List<EnvVar> existingEnvs, List<ContainerEnvVar> containerEnvs) {
        if (containerEnvs != null) {
            // Create set of env var names to test if any user defined template env vars will conflict with those set above
            Set<String> predefinedEnvs = new HashSet<>();
            for (EnvVar envVar : existingEnvs) {
                predefinedEnvs.add(envVar.getName());
            }

            // Set custom env vars from the user defined template
            for (ContainerEnvVar containerEnvVar : containerEnvs) {
                if (predefinedEnvs.contains(containerEnvVar.getName())) {
                    LOGGER.warnCr(reconciliation, "User defined container template environment variable {} is already in use and will be ignored",  containerEnvVar.getName());
                } else {
                    existingEnvs.add(buildEnvVar(containerEnvVar.getName(), containerEnvVar.getValue()));
                }
            }
        }
    }

    protected ClusterRoleBinding getClusterRoleBinding(String name, Subject subject, RoleRef roleRef) {
        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(templateClusterRoleBindingLabels).toMap())
                    .withAnnotations(templateClusterRoleBindingAnnotations)
                .endMetadata()
                .withSubjects(subject)
                .withRoleRef(roleRef)
                .build();
    }

    /**
     * Adds warning condition to the list of warning conditions
     *
     * @param warning  Condition which will be added to the warning conditions
     */
    public void addWarningCondition(Condition warning) {
        warningConditions.add(warning);
    }

    /**
     * Returns a list of warning conditions set by the model. Returns an empty list if no warning conditions were set.
     *
     * @return  List of warning conditions.
     */
    public List<Condition> getWarningConditions() {
        return warningConditions;
    }

    public DeploymentStrategy getDeploymentStrategy() {
        if (templateDeploymentStrategy == io.strimzi.api.kafka.model.template.DeploymentStrategy.ROLLING_UPDATE) {
            return new DeploymentStrategyBuilder()
                    .withType("RollingUpdate")
                    .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                            .withMaxSurge(new IntOrString(1))
                            .withMaxUnavailable(new IntOrString(0))
                            .build())
                    .build();
        } else {
            return new DeploymentStrategyBuilder()
                    .withType("Recreate")
                    .build();
        }
    }

    protected Volume createTempDirVolume() {
        return createTempDirVolume(STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME);
    }

    protected Volume createTempDirVolume(String volumeName) {
        return VolumeUtils.createEmptyDirVolume(volumeName, templateTmpDirSizeLimit == null ? STRIMZI_TMP_DIRECTORY_DEFAULT_SIZE : templateTmpDirSizeLimit, "Memory");
    }

    protected VolumeMount createTempDirVolumeMount() {
        return createTempDirVolumeMount(STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME);
    }

    protected VolumeMount createTempDirVolumeMount(String volumeName) {
        return VolumeUtils.createVolumeMount(volumeName, STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH);
    }
}
