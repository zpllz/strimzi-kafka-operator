/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SANITY;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@ParallelSuite
public class CruiseControlST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);
    private static final String CRUISE_CONTROL_METRICS_TOPIC = "__strimzi_cruisecontrol_metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "__strimzi_cruisecontrol_modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "__strimzi_cruisecontrol_partitionmetricsamples"; // partitions 32 , rf - 2

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(CruiseControlST.class.getSimpleName()).stream().findFirst().get();

    @IsolatedTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testAutoCreationOfCruiseControlTopicsWithResources(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editOrNewSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
                .editCruiseControl()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("300Mi"))
                        .addToRequests("memory", new Quantity("300Mi"))
                        .build())
                    .withNewJvmOptions()
                        .withXmx("200M")
                        .withXms("128M")
                        .withXx(Map.of("UseG1GC", "true"))
                    .endJvmOptions()
                .endCruiseControl()
            .endSpec()
            .build());

        String ccPodName = kubeClient().listPodsByPrefixInName(namespace, CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        Container container = (Container) KubeClusterResource.kubeClient(namespace).getPod(namespace, ccPodName).getSpec().getContainers().stream().filter(c -> c.getName().equals("cruise-control")).findFirst().get();
        assertThat(container.getResources().getLimits().get("memory"), is(new Quantity("300Mi")));
        assertThat(container.getResources().getRequests().get("memory"), is(new Quantity("300Mi")));
        assertExpectedJavaOpts(namespace, ccPodName, "cruise-control",
                "-Xmx200M", "-Xms128M", "-XX:+UseG1GC");

        KafkaTopicUtils.waitForKafkaTopicReady(namespace, CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicSpec metricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace)
            .withName(CRUISE_CONTROL_METRICS_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(namespace, CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicSpec modelTrainingTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace)
            .withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(namespace, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        KafkaTopicSpec partitionMetricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace)
            .withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get().getSpec();

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_METRICS_TOPIC);
        assertThat(metricsTopic.getPartitions(), is(1));
        assertThat(metricsTopic.getReplicas(), is(3));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        assertThat(modelTrainingTopic.getPartitions(), is(32));
        assertThat(modelTrainingTopic.getReplicas(), is(2));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        assertThat(partitionMetricsTopic.getPartitions(), is(32));
        assertThat(partitionMetricsTopic.getReplicas(), is(2));
    }

    @IsolatedTest
    void testCruiseControlWithApiSecurityDisabled(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
                .editMetadata()
                .withNamespace(namespace)
                .endMetadata()
                .editOrNewSpec()
                    .editCruiseControl()
                        .addToConfig("webserver.security.enable", "false")
                        .addToConfig("webserver.ssl.enable", "false")
                    .endCruiseControl()
                .endSpec()
                .build());
        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(clusterName)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespace, clusterName, KafkaRebalanceState.ProposalReady);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    void testCruiseControlWithRebalanceResourceAndRefreshAnnotation(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build());
        resourceManager.createResource(extensionContext, false,  KafkaRebalanceTemplates.kafkaRebalance(clusterName)
                .editMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build());

        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespace, clusterName, KafkaRebalanceState.NotReady);

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespace, kafkaSelector);

        // Cruise Control spec is now enabled
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().setCruiseControl(new CruiseControlSpec()), namespace);

        RollingUpdateUtils.waitTillComponentHasRolled(namespace, kafkaSelector, 3, kafkaPods);

        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespace, clusterName), namespace, clusterName);

        LOGGER.info("Annotating KafkaRebalance: {} with 'refresh' anno", clusterName);
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespace, clusterName), namespace, clusterName, KafkaRebalanceAnnotation.refresh);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespace, clusterName, KafkaRebalanceState.ProposalReady);

        LOGGER.info("Trying rebalancing process again");
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespace, clusterName), namespace, clusterName);
    }

    @ParallelNamespaceTest
    void testCruiseControlWithSingleNodeKafka(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        final String errMessage =  "Kafka " + namespaceName + "/" + clusterName + " has invalid configuration." +
            " Cruise Control cannot be deployed with a single-node Kafka cluster. It requires " +
            "at least two Kafka nodes.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaWithCruiseControl(clusterName, 1, 1).build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, namespaceName, errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();

        assertThat(kafkaStatus.getConditions().stream().filter(c -> "InvalidResourceException".equals(c.getReason())).findFirst().orElse(null), is(notNullValue()));

        LOGGER.info("Increasing Kafka nodes to 3");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().getKafka().setReplicas(3), namespaceName);
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);

        kafkaStatus = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testCruiseControlTopicExclusion(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        final String excludedTopic1 = "excluded-topic-1";
        final String excludedTopic2 = "excluded-topic-2";
        final String includedTopic = "included-topic";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, excludedTopic1, namespaceName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, excludedTopic2, namespaceName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, includedTopic, namespaceName).build());

        resourceManager.createResource(extensionContext,  KafkaRebalanceTemplates.kafkaRebalance(clusterName)
            .editOrNewSpec()
                .withExcludedTopics("excluded-.*")
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(namespaceName).withName(clusterName).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic1));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic2));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), not(containsString(includedTopic)));

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespace, clusterName), namespaceName, clusterName, KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.Ready);
    }

    @ParallelNamespaceTest
    void testCruiseControlReplicaMovementStrategy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        final String replicaMovementStrategies = "default.replica.movement.strategies";
        final String newReplicaMovementStrategies = "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy," +
            "com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        String ccPodName = kubeClient().listPodsByPrefixInName(namespaceName, CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Check for default CruiseControl replicaMovementStrategy in pod configuration file.");
        Map<String, Object> actualStrategies = KafkaResource.kafkaClient().inNamespace(namespaceName)
            .withName(clusterName).get().getSpec().getCruiseControl().getConfig();
        assertThat(actualStrategies, anEmptyMap());

        String ccConfFileContent = cmdKubeClient(namespaceName).execInPodContainer(ccPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "cat", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, not(containsString(replicaMovementStrategies)));

        Map<String, String> kafkaRebalanceSnapshot = DeploymentUtils.depSnapshot(namespaceName, CruiseControlResources.deploymentName(clusterName));

        Map<String, Object> ccConfigMap = new HashMap<>();
        ccConfigMap.put(replicaMovementStrategies, newReplicaMovementStrategies);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Set non-default CruiseControl replicaMovementStrategies to KafkaRebalance resource.");
            kafka.getSpec().getCruiseControl().setConfig(ccConfigMap);
        }, namespaceName);

        LOGGER.info("Verifying that CC pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(namespaceName, CruiseControlResources.deploymentName(clusterName), 1, kafkaRebalanceSnapshot);

        ccPodName = kubeClient().listPodsByPrefixInName(namespaceName, CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        ccConfFileContent = cmdKubeClient(namespaceName).execInPodContainer(ccPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "cat", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, containsString(newReplicaMovementStrategies));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testCruiseControlIntraBrokerBalancing(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String diskSize = "6Gi";

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSize).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSize).build()
                ).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                    .editOrNewSpec()
                        .editKafka()
                            .withStorage(jbodStorage)
                        .endKafka()
                    .endSpec()
                .build());
        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                    .editOrNewSpec()
                        .withRebalanceDisk(true)
                    .endSpec()
                .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        // The provision status should be "UNDECIDED" when doing an intra-broker disk balance because it is irrelevant to the provision status
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("provisionStatus").toString(), containsString("UNDECIDED"));
    }

    @ParallelNamespaceTest
    void testCruiseControlIntraBrokerBalancingWithoutSpecifyingJBODStorage(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());
        resourceManager.createResource(extensionContext, false, KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editOrNewSpec()
                .withRebalanceDisk(true)
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.NotReady);

        // The intra-broker rebalance will fail for Kafka clusters with a non-JBOD configuration.
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus();
        assertThat(kafkaRebalanceStatus.getConditions().get(0).getReason(), is("InvalidResourceException"));
    }

    @IsolatedTest
    @KRaftNotSupported("Scale-up / scale-down not working in KRaft mode - https://github.com/strimzi/strimzi-kafka-operator/issues/6862")
    void testCruiseControlDuringBrokerScaleUpAndDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);
        final int initialReplicas = 3;
        final int scaleTo = 5;

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), initialReplicas, initialReplicas)
                .editOrNewMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 10, 3)
                .editOrNewMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Checking that {} topic has replicas on first 3 brokers", testStorage.getTopicName());
        List<String> topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertEquals(0, (int) topicReplicas.stream().filter(line -> line.contains("3") || line.contains("4")).count());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        LOGGER.info("Scaling Kafka up to {}", scaleTo);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(scaleTo), testStorage.getNamespaceName());
        kafkaPods = RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaleTo, kafkaPods);

        LOGGER.info("Creating KafkaRebalance with add_brokers mode");

        // when using add_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResource(extensionContext, false,
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editOrNewMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.ADD_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).delete();

        LOGGER.info("Checking that {} topic has replicas on one of the new brokers (or both)", testStorage.getTopicName());
        topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertTrue(topicReplicas.stream().anyMatch(line -> line.contains("3") || line.contains("4")));

        LOGGER.info("Creating KafkaRebalance with remove_brokers mode - it needs to be done before actual scaling down of Kafka pods");

        // when using remove_brokers mode, we can hit `ProposalReady` right after KR creation - that's why `waitReady` is set to `false` here
        resourceManager.createResource(extensionContext, false,
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
                .editOrNewMetadata()
                    .withNamespace(namespace)
                .endMetadata()
                .editOrNewSpec()
                    .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                    .withBrokers(3, 4)
                .endSpec()
                .build()
        );

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(),  testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        KafkaRebalanceUtils.doRebalancingProcess(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Checking that {} topic has replicas only on first 3 brokers", testStorage.getTopicName());
        topicReplicas = KafkaTopicUtils.getKafkaTopicReplicasForEachPartition(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertEquals(0, (int) topicReplicas.stream().filter(line -> line.contains("3") || line.contains("4")).count());

        LOGGER.info("Scaling Kafka down to {}", initialReplicas);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setReplicas(initialReplicas), testStorage.getNamespaceName());
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), initialReplicas, kafkaPods);
    }
}
