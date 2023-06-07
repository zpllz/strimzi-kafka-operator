/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * ModelUtils is a utility class that holds generic static helper functions
 * These are generally to be used within the classes that extend the AbstractModel class
 */
public class ModelUtils {

    private ModelUtils() {}

    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ModelUtils.class.getName());
    public static final String TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";
    public static final String DEFAULT_SECRET_DATA = "qingcloud";

    /**
     * @param certificateAuthority The CA configuration.
     * @return The cert validity.
     */
    public static int getCertificateValidity(CertificateAuthority certificateAuthority) {
        int validity = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        if (certificateAuthority != null
                && certificateAuthority.getValidityDays() > 0) {
            validity = certificateAuthority.getValidityDays();
        }
        return validity;
    }

    /**
     * @param certificateAuthority The CA configuration.
     * @return The renewal days.
     */
    public static int getRenewalDays(CertificateAuthority certificateAuthority) {
        int renewalDays = CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS;

        if (certificateAuthority != null
                && certificateAuthority.getRenewalDays() > 0) {
            renewalDays = certificateAuthority.getRenewalDays();
        }

        return renewalDays;
    }

    /**
     * Generate labels used by entity-operators to find the resources related to given cluster
     *
     * @param cluster   Name of the cluster
     * @return  Map with label definition
     */
    public static String defaultResourceLabels(String cluster) {
        return String.format("%s=%s",
                Labels.STRIMZI_CLUSTER_LABEL, cluster);
    }

    static EnvVar tlsSidecarLogEnvVar(TlsSidecar tlsSidecar) {
        return AbstractModel.buildEnvVar(TLS_SIDECAR_LOG_LEVEL,
                (tlsSidecar != null && tlsSidecar.getLogLevel() != null ?
                        tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE).toValue());
    }

    public static Secret buildSecret(Reconciliation reconciliation, ClusterCa clusterCa, Secret secret, String namespace, String secretName,
                                     String commonName, String keyCertName, Labels labels, OwnerReference ownerReference, boolean isMaintenanceTimeWindowsSatisfied) {
        Map<String, String> data = new HashMap<>(4);
        CertAndKey certAndKey = null;
        boolean shouldBeRegenerated = false;
        List<String> reasons = new ArrayList<>(2);

        if (secret == null) {
            reasons.add("certificate doesn't exist yet");
            shouldBeRegenerated = true;
        } else {
            if (clusterCa.keyCreated() || clusterCa.certRenewed() ||
                    (isMaintenanceTimeWindowsSatisfied && clusterCa.isExpiring(secret, keyCertName + ".crt")) ||
                    clusterCa.hasCaCertGenerationChanged(secret)) {
                reasons.add("certificate needs to be renewed");
                shouldBeRegenerated = true;
            }
        }

        if (shouldBeRegenerated) {
            LOGGER.debugCr(reconciliation, "Certificate for pod {} need to be regenerated because: {}", keyCertName, String.join(", ", reasons));

            try {
                certAndKey = clusterCa.generateSignedCert(commonName, Ca.IO_STRIMZI);
            } catch (IOException e) {
                LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            }

            LOGGER.debugCr(reconciliation, "End generating certificates");
        } else {
            if (secret.getData().get(keyCertName + ".p12") != null &&
                    !secret.getData().get(keyCertName + ".p12").isEmpty() &&
                    secret.getData().get(keyCertName + ".password") != null &&
                    !secret.getData().get(keyCertName + ".password").isEmpty()) {
                certAndKey = new CertAndKey(
                        decodeFromSecret(secret, keyCertName + ".key"),
                        decodeFromSecret(secret, keyCertName + ".crt"),
                        null,
                        decodeFromSecret(secret, keyCertName + ".p12"),
                        new String(decodeFromSecret(secret, keyCertName + ".password"), StandardCharsets.US_ASCII)
                );
            } else {
                try {
                    // coming from an older operator version, the secret exists but without keystore and password
                    certAndKey = clusterCa.addKeyAndCertToKeyStore(commonName,
                            decodeFromSecret(secret, keyCertName + ".key"),
                            decodeFromSecret(secret, keyCertName + ".crt"));
                } catch (IOException e) {
                    LOGGER.errorCr(reconciliation, "Error generating the keystore for {}", keyCertName, e);
                }
            }
        }

        if (certAndKey != null) {
            data.put(keyCertName + ".key", certAndKey.keyAsBase64String());
            data.put(keyCertName + ".crt", certAndKey.certAsBase64String());
            data.put(keyCertName + ".p12", certAndKey.keyStoreAsBase64String());
            data.put(keyCertName + ".password", certAndKey.storePasswordAsBase64String());
        }

        return createSecret(secretName, namespace, labels, ownerReference, data,
                Collections.singletonMap(clusterCa.caCertGenerationAnnotation(), String.valueOf(clusterCa.certGeneration())), emptyMap());
    }

    public static Secret createSecret(String name, String namespace, Labels labels, OwnerReference ownerReference,
                                      Map<String, String> data, Map<String, String> customAnnotations, Map<String, String> customLabels) {

        if (ownerReference == null) {
            return new SecretBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), customLabels))
                        .withAnnotations(customAnnotations)
                    .endMetadata()
                    .withType("Opaque")
                    .withData(data)
                    .build();
        } else {
            return new SecretBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withOwnerReferences(ownerReference)
                        .withNamespace(namespace)
                        .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), customLabels))
                        .withAnnotations(customAnnotations)
                    .endMetadata()
                    .withType("Opaque")
                    .withData(data)
                    .build();
        }
    }


    public static String secretEncryptionStr(String data) {
        String decryptedStringEncode = new String("init");
        String[] command = {"bash", "-c", String.format("echo \"%s\" | openssl enc -aes256 -iter 20000 -pbkdf2 -base64 -k %s -salt", data, DEFAULT_SECRET_DATA)};
        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            String line;
            StringBuilder output = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                output.append(line);
            }
            decryptedStringEncode = Base64.getEncoder().encodeToString(output.toString().getBytes());
        } catch (IOException a) {
            LOGGER.errorOp(String.format("run encode secret error %s", a));
        }
        return decryptedStringEncode;
    }

    /**
     * Parses the values from the PodDisruptionBudgetTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodDisruptionBudgetTemplate should be set
     * @param pdb PodDisruptionBudgetTemplate with the values form the CRD
     */
    public static void parsePodDisruptionBudgetTemplate(AbstractModel model, PodDisruptionBudgetTemplate pdb)   {
        if (pdb != null)  {
            if (pdb.getMetadata() != null) {
                model.templatePodDisruptionBudgetLabels = pdb.getMetadata().getLabels();
                model.templatePodDisruptionBudgetAnnotations = pdb.getMetadata().getAnnotations();
            }

            model.templatePodDisruptionBudgetMaxUnavailable = pdb.getMaxUnavailable();
        }
    }

    /**
     * Parses the values from the PodTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodTemplate should be set
     * @param pod PodTemplate with the values form the CRD
     */
    public static void parsePodTemplate(AbstractModel model, PodTemplate pod)   {
        if (pod != null)  {
            if (pod.getMetadata() != null) {
                model.templatePodLabels = pod.getMetadata().getLabels();
                model.templatePodAnnotations = pod.getMetadata().getAnnotations();
            }

            if (pod.getAffinity() != null)  {
                model.setUserAffinity(pod.getAffinity());
            }

            if (pod.getTolerations() != null)   {
                model.setTolerations(removeEmptyValuesFromTolerations(pod.getTolerations()));
            }

            model.templateTerminationGracePeriodSeconds = pod.getTerminationGracePeriodSeconds();
            model.templateImagePullSecrets = pod.getImagePullSecrets();
            model.templateSecurityContext = pod.getSecurityContext();
            model.templatePodPriorityClassName = pod.getPriorityClassName();
            model.templatePodSchedulerName = pod.getSchedulerName();
            model.templatePodHostAliases = pod.getHostAliases();
            model.templatePodTopologySpreadConstraints = pod.getTopologySpreadConstraints();
            model.templatePodEnableServiceLinks = pod.getEnableServiceLinks();
            model.templateTmpDirSizeLimit = pod.getTmpDirSizeLimit();
        }
    }

    /**
     * Parses the values from the InternalServiceTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodTemplate should be set
     * @param service InternalServiceTemplate with the values form the CRD
     */
    public static void parseInternalServiceTemplate(AbstractModel model, InternalServiceTemplate service)   {
        if (service != null)  {
            if (service.getMetadata() != null) {
                model.templateServiceLabels = service.getMetadata().getLabels();
                model.templateServiceAnnotations = service.getMetadata().getAnnotations();
            }

            model.templateServiceIpFamilyPolicy = service.getIpFamilyPolicy();
            model.templateServiceIpFamilies = service.getIpFamilies();
        }
    }

    /**
     * Parses the values from the InternalServiceTemplate of a headless service in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodTemplate should be set
     * @param service InternalServiceTemplate with the values form the CRD
     */
    public static void parseInternalHeadlessServiceTemplate(AbstractModel model, InternalServiceTemplate service)   {
        if (service != null)  {
            if (service.getMetadata() != null) {
                model.templateHeadlessServiceLabels = service.getMetadata().getLabels();
                model.templateHeadlessServiceAnnotations = service.getMetadata().getAnnotations();
            }

            model.templateHeadlessServiceIpFamilyPolicy = service.getIpFamilyPolicy();
            model.templateHeadlessServiceIpFamilies = service.getIpFamilies();
        }
    }

    /**
     * Parses the values from the DeploymentTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the DeploymentTemplate should be set
     * @param template DeploymentTemplate with the values form the CRD
     */
    public static void parseDeploymentTemplate(AbstractModel model, DeploymentTemplate template)   {
        if (template != null) {
            if (template.getMetadata() != null) {
                model.templateDeploymentLabels = template.getMetadata().getLabels();
                model.templateDeploymentAnnotations = template.getMetadata().getAnnotations();
            }

            if (template.getDeploymentStrategy() != null)   {
                model.templateDeploymentStrategy = template.getDeploymentStrategy();
            } else {
                model.templateDeploymentStrategy = io.strimzi.api.kafka.model.template.DeploymentStrategy.ROLLING_UPDATE;
            }
        }
    }

    /**
     * Returns whether the given {@code Storage} instance is a persistent claim one or
     * a JBOD containing at least one persistent volume.
     *
     * @param storage the Storage instance to check
     * @return Whether the give Storage contains any persistent storage.
     */
    public static boolean containsPersistentStorage(Storage storage) {
        boolean isPersistentClaimStorage = storage instanceof PersistentClaimStorage;

        if (!isPersistentClaimStorage && storage instanceof JbodStorage) {
            isPersistentClaimStorage |= ((JbodStorage) storage).getVolumes()
                    .stream().anyMatch(volume -> volume instanceof PersistentClaimStorage);
        }
        return isPersistentClaimStorage;
    }

    public static Storage decodeStorageFromJson(String json) {
        try {
            return new ObjectMapper().readValue(json, Storage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String encodeStorageToJson(Storage storage) {
        try {
            return new ObjectMapper().writeValueAsString(storage);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a map with custom labels or annotations from an environment variable
     *
     * @param envVarName Name of the environment variable which should be used as input
     *
     * @return A map with labels or annotations
     */
    public static Map<String, String> getCustomLabelsOrAnnotations(String envVarName)   {
        return Util.parseMap(System.getenv().get(envVarName));
    }

    private static byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Compares two Secrets with certificates and checks whether any value for a key which exists in both Secrets
     * changed. This method is used to evaluate whether rolling update of existing brokers is needed when secrets with
     * certificates change. It separates changes for existing certificates with other changes to the secret such as
     * added or removed certificates (scale-up or scale-down).
     *
     * @param current   Existing secret
     * @param desired   Desired secret
     *
     * @return  True if there is a key which exists in the data sections of both secrets and which changed.
     */
    public static boolean doExistingCertificatesDiffer(Secret current, Secret desired) {
        Map<String, String> currentData = current.getData();
        Map<String, String> desiredData = desired.getData();

        if (currentData == null) {
            return true;
        } else {
            for (Map.Entry<String, String> entry : currentData.entrySet()) {
                String desiredValue = desiredData.get(entry.getKey());
                if (entry.getValue() != null
                        && desiredValue != null
                        && !entry.getValue().equals(desiredValue)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static <T> List<T> asListOrEmptyList(List<T> list) {
        return Optional.ofNullable(list)
                .orElse(Collections.emptyList());
    }

    private static String getJavaSystemPropertiesToString(List<SystemProperty> javaSystemProperties) {
        if (javaSystemProperties == null) {
            return null;
        }
        List<String> javaSystemPropertiesList = new ArrayList<>(javaSystemProperties.size());
        for (SystemProperty property: javaSystemProperties) {
            javaSystemPropertiesList.add("-D" + property.getName() + "=" + property.getValue());
        }
        return String.join(" ", javaSystemPropertiesList);
    }

    /**
     * This method transforms a String into a List of Strings, where each entry is an uncommented line of input.
     * The lines beginning with '#' (comments) are ignored.
     * @param config ConfigMap data as a String
     * @return List of String key=value
     */
    public static List<String> getLinesWithoutCommentsAndEmptyLines(String config) {
        List<String> validLines = new ArrayList<>();
        if (config != null) {
            List<String> allLines = Arrays.asList(config.split("\\r?\\n"));

            for (String line : allLines) {
                if (!line.isEmpty() && !line.matches("\\s*\\#.*")) {
                    validLines.add(line);
                }
            }
        }
        return validLines;
    }

    /**
     * Get the set of JVM options, bringing the Java system properties as well, and fill corresponding Strimzi environment variables
     * in order to pass them to the running application on the command line
     *
     * @param envVars environment variables list to put the JVM options and Java system properties
     * @param jvmOptions JVM options
     */
    public static void javaOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        StringBuilder strimziJavaOpts = new StringBuilder();
        String xms = jvmOptions != null ? jvmOptions.getXms() : null;
        if (xms != null) {
            strimziJavaOpts.append("-Xms").append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            strimziJavaOpts.append(" -Xmx").append(xmx);
        }

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                strimziJavaOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    strimziJavaOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    strimziJavaOpts.append("-").append(k);
                } else  {
                    strimziJavaOpts.append(k).append("=").append(v);
                }
            });
        }

        String optsTrim = strimziJavaOpts.toString().trim();
        if (!optsTrim.isEmpty()) {
            envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS, optsTrim));
        }

        List<SystemProperty> javaSystemProperties = jvmOptions != null ? jvmOptions.getJavaSystemProperties() : null;
        if (javaSystemProperties != null) {
            String propsTrim = ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties).trim();
            if (!propsTrim.isEmpty()) {
                envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, propsTrim));
            }
        }
    }

    /**
     * Adds the STRIMZI_JAVA_SYSTEM_PROPERTIES variable to the EnvVar list if any system properties were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmSystemProperties(List<EnvVar> envVars, JvmOptions jvmOptions) {
        if (jvmOptions != null) {
            String jvmSystemPropertiesString = ModelUtils.getJavaSystemPropertiesToString(jvmOptions.getJavaSystemProperties());
            if (jvmSystemPropertiesString != null && !jvmSystemPropertiesString.isEmpty()) {
                envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, jvmSystemPropertiesString));
            }
        }
    }

    /**
     * Adds the KAFKA_JVM_PERFORMANCE_OPTS variable to the EnvVar list if any performance related options were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmPerformanceOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        StringBuilder jvmPerformanceOpts = new StringBuilder();

        Map<String, String> xx = jvmOptions != null ? jvmOptions.getXx() : null;
        if (xx != null) {
            xx.forEach((k, v) -> {
                jvmPerformanceOpts.append(' ').append("-XX:");

                if ("true".equalsIgnoreCase(v))   {
                    jvmPerformanceOpts.append("+").append(k);
                } else if ("false".equalsIgnoreCase(v)) {
                    jvmPerformanceOpts.append("-").append(k);
                } else  {
                    jvmPerformanceOpts.append(k).append("=").append(v);
                }
            });
        }

        String jvmPerformanceOptsString = jvmPerformanceOpts.toString().trim();
        if (!jvmPerformanceOptsString.isEmpty()) {
            envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS, jvmPerformanceOptsString));
        }
    }

    /**
     * Adds KAFKA_HEAP_OPTS variable to the EnvVar list if any heap related options were specified through the provided JVM options
     * If Xmx Java Options are not set STRIMZI_DYNAMIC_HEAP_PERCENTAGE and STRIMZI_DYNAMIC_HEAP_MAX may also be set by using the ResourceRequirements
     *
     * @param envVars list of the Environment Variables to add to
     * @param dynamicHeapPercentage value to set for the STRIMZI_DYNAMIC_HEAP_PERCENTAGE
     * @param dynamicHeapMaxBytes value to set for the STRIMZI_DYNAMIC_HEAP_MAX
     * @param jvmOptions JVM options
     * @param resources the resource requirements
     */
    public static void heapOptions(List<EnvVar> envVars, int dynamicHeapPercentage, long dynamicHeapMaxBytes, JvmOptions jvmOptions, ResourceRequirements resources) {
        if (dynamicHeapPercentage <= 0 || dynamicHeapPercentage > 100)  {
            throw new RuntimeException("The Heap percentage " + dynamicHeapPercentage + " is invalid. It has to be >0 and <= 100.");
        }

        StringBuilder kafkaHeapOpts = new StringBuilder();

        String xms = jvmOptions != null ? jvmOptions.getXms() : null;
        if (xms != null) {
            kafkaHeapOpts.append("-Xms")
                    .append(xms);
        }

        String xmx = jvmOptions != null ? jvmOptions.getXmx() : null;
        if (xmx != null) {
            // Honour user provided explicit max heap
            kafkaHeapOpts.append(' ').append("-Xmx").append(xmx);
        } else {
            // Get the resources => if requests are set, take request. If requests are not set, try limits
            Quantity configuredMemory = null;
            if (resources != null)  {
                if (resources.getRequests() != null && resources.getRequests().get("memory") != null)    {
                    configuredMemory = resources.getRequests().get("memory");
                } else if (resources.getLimits() != null && resources.getLimits().get("memory") != null)   {
                    configuredMemory = resources.getLimits().get("memory");
                }
            }

            if (configuredMemory != null) {
                // Delegate to the container to figure out only when CGroup memory limits are defined to prevent allocating
                // too much memory on the kubelet.

                envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE, Integer.toString(dynamicHeapPercentage)));

                if (dynamicHeapMaxBytes > 0) {
                    envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX, Long.toString(dynamicHeapMaxBytes)));
                }
            } else if (xms == null) {
                // When no memory limit, `Xms`, and `Xmx` are defined then set a default `Xms` and
                // leave `Xmx` undefined.
                kafkaHeapOpts.append("-Xms").append(AbstractModel.DEFAULT_JVM_XMS);
            }
        }

        String kafkaHeapOptsString = kafkaHeapOpts.toString().trim();
        if (!kafkaHeapOptsString.isEmpty()) {
            envVars.add(AbstractModel.buildEnvVar(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS, kafkaHeapOptsString));
        }
    }

    /**
     * If the toleration.value is an empty string, set it to null. That solves an issue when built STS contains a filed
     * with an empty property value. K8s is removing properties like this and thus we cannot fetch an equal STS which was
     * created with (some) empty value.
     *
     * @param tolerations   Tolerations list to check whether toleration.value is an empty string and eventually replace it by null
     *
     * @return              List of tolerations with fixed empty strings
     */
    public static List<Toleration> removeEmptyValuesFromTolerations(List<Toleration> tolerations) {
        if (tolerations != null) {
            tolerations.stream().filter(toleration -> toleration.getValue() != null && toleration.getValue().isEmpty()).forEach(emptyValTol -> emptyValTol.setValue(null));
            return tolerations;
        } else {
            return null;
        }
    }

    /**
     *
     * @param builder the builder which is used to populate the node affinity
     * @param userAffinity the userAffinity which is defined by the user
     * @param topologyKey  the topology key which is used to select the node
     * @return the AffinityBuilder which has the node selector with topology key which is needed to make sure
     * the pods are scheduled only on nodes with the rack label
     */
    public static AffinityBuilder populateAffinityBuilderWithRackLabelSelector(AffinityBuilder builder, Affinity userAffinity, String topologyKey) {
        // We need to add node affinity to make sure the pods are scheduled only on nodes with the rack label
        NodeSelectorRequirement selector = new NodeSelectorRequirementBuilder()
                .withOperator("Exists")
                .withKey(topologyKey)
                .build();

        if (userAffinity != null
                && userAffinity.getNodeAffinity() != null
                && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() != null
                && userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms() != null) {
            // User has specified some Node Selector Terms => we should enhance them
            List<NodeSelectorTerm> oldTerms = userAffinity.getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution().getNodeSelectorTerms();
            List<NodeSelectorTerm> enhancedTerms = new ArrayList<>(oldTerms.size());

            for (NodeSelectorTerm term : oldTerms) {
                NodeSelectorTerm enhancedTerm = new NodeSelectorTermBuilder(term)
                        .addToMatchExpressions(selector)
                        .build();
                enhancedTerms.add(enhancedTerm);
            }

            builder = builder
                    .editOrNewNodeAffinity()
                        .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .withNodeSelectorTerms(enhancedTerms)
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity();
        } else {
            // User has not specified any selector terms => we add our own
            builder = builder
                    .editOrNewNodeAffinity()
                        .editOrNewRequiredDuringSchedulingIgnoredDuringExecution()
                            .addNewNodeSelectorTerm()
                                .withMatchExpressions(selector)
                            .endNodeSelectorTerm()
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endNodeAffinity();
        }
        return builder;
    }

    /**
     * Decides whether the Cluster Operator needs namespaceSelector to be configured in the network policies in order
     * to talk with the operands. This follows the following rules:
     *     - If it runs in the same namespace as the operand, do not set namespace selector
     *     - If it runs in a different namespace, but user provided selector labels, use the labels
     *     - If it runs in a different namespace, and user didn't provided selector labels, open it to COs in all namespaces
     *
     * @param peer                      Network policy peer where the namespace selector should be set
     * @param operandNamespace          Namespace of the operand
     * @param operatorNamespace         Namespace of the Strimzi CO
     * @param operatorNamespaceLabels   Namespace labels provided by the user
     */
    public static void setClusterOperatorNetworkPolicyNamespaceSelector(NetworkPolicyPeer peer, String operandNamespace, String operatorNamespace, Labels operatorNamespaceLabels)   {
        if (!operandNamespace.equals(operatorNamespace)) {
            // If CO and the operand do not run in the same namespace, we need to handle cross namespace access

            if (operatorNamespaceLabels != null && !operatorNamespaceLabels.toMap().isEmpty())    {
                // If user specified the namespace labels, we can use them to make the network policy as tight as possible
                LabelSelector nsLabelSelector = new LabelSelector();
                nsLabelSelector.setMatchLabels(operatorNamespaceLabels.toMap());
                peer.setNamespaceSelector(nsLabelSelector);
            } else {
                // If no namespace labels were specified, we open the network policy to COs in all namespaces
                peer.setNamespaceSelector(new LabelSelector());
            }
        }
    }

    /**
     * Checks if the section of the custom resource has any metrics configuration and sets it in the AbstractModel.
     *
     * @param model                     The cluster model where the metrics will be configured
     * @param resourceWithMetrics       The section of the resource with metrics configuration
     */
    public static void parseMetrics(AbstractModel model, HasConfigurableMetrics resourceWithMetrics)   {
        if (resourceWithMetrics.getMetricsConfig() != null)    {
            model.setMetricsEnabled(true);
            model.setMetricsConfigInCm(resourceWithMetrics.getMetricsConfig());
        }
    }

    /**
     * Creates the OwnerReference based on the resource passed as parameter
     *
     * @param owner     The resource which should be the owner
     *
     * @return          The new OwnerReference
     */
    public static OwnerReference createOwnerReference(HasMetadata owner)   {
        return new OwnerReferenceBuilder()
                .withApiVersion(owner.getApiVersion())
                .withKind(owner.getKind())
                .withName(owner.getMetadata().getName())
                .withUid(owner.getMetadata().getUid())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
    }

    /**
     * Checks whether the resource has given Owner Reference among its list of owner references
     *
     * @param resource  Resource which should be checked for OwnerReference
     * @param owner     OwnerReference which should be verified
     *
     * @return          True if the owner reference is found. False otherwise.
     */
    public static boolean hasOwnerReference(HasMetadata resource, OwnerReference owner)    {
        if (resource.getMetadata().getOwnerReferences() != null) {
            return resource.getMetadata().getOwnerReferences()
                    .stream()
                    .anyMatch(o -> owner.getApiVersion().equals(o.getApiVersion())
                            && owner.getKind().equals(o.getKind())
                            && owner.getName().equals(o.getName())
                            && owner.getUid().equals(o.getUid()));
        } else {
            return false;
        }
    }

    /**
     * Extracts the CA generation from the CA
     *
     * @param ca    CA from which the generation should be extracted
     *
     * @return      CA generation or the initial generation if no generation is set
     */
    public static int caCertGeneration(Ca ca) {
        return Annotations.intAnnotation(ca.caCertSecret(), Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, Ca.INIT_GENERATION);
    }

    /**
     * Returns the id of a pod given the pod name
     *
     * @param podName   Name of pod
     *
     * @return          Id of the pod
     */
    public static int idOfPod(String podName)  {
        return Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
    }
}
