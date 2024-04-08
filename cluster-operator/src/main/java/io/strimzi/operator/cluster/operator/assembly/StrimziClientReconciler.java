/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.StrimziClientResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.StrimziClient;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.vertx.core.Future;

import java.time.Clock;
import java.util.List;

/**
 * Class used for reconciliation of Strimzi Client. This class contains both the steps of the Kafka Exporter
 * reconciliation pipeline and is also used to store the state between them.
 */
public class StrimziClientReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(StrimziClientReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final StrimziClient strimziClient;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;

    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;

    boolean existingKafkaExporterCertsChanged;


    /**
     * Constructs the Strimzi Client reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param clusterCa                 The Cluster CA instance
     */
    public StrimziClientReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            ClusterCa clusterCa
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.strimziClient = StrimziClient.fromCrd(reconciliation, kafkaAssembly, versions);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();

        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;

        this.existingKafkaExporterCertsChanged = false;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     * @param clock             The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                          That time is used for checking maintenance windows
     *
     * @return                  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Clock clock)    {
        return serviceAccount()
                .compose(i -> certificatesSecret(clock))
                .compose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Strimzi Client Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        StrimziClientResources.deploymentName(reconciliation.name()),
                        strimziClient != null ? strimziClient.generateServiceAccount() : null
                ).map((Void) null);
    }

    /**
     * Manages the Strimzi Client Secret with certificates.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Future which completes when the reconciliation is done
     */
    private Future<Void> certificatesSecret(Clock clock) {
        return Future.succeededFuture();
    }

    /**
     * Manages the Strimzi Client deployment.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (strimziClient != null) {
            Deployment deployment = strimziClient.generateDeployment(isOpenShift, imagePullPolicy, imagePullSecrets);
            int caCertGeneration = ModelUtils.caCertGeneration(this.clusterCa);
            Annotations.annotations(deployment.getSpec().getTemplate()).put(
                    Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(caCertGeneration));

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), StrimziClientResources.deploymentName(reconciliation.name()), deployment)
                    .compose(patchResult -> {
                        if (patchResult instanceof ReconcileResult.Noop)   {
                            // Deployment needs ot be rolled because the certificate secret changed or older/expired cluster CA removed
                            if (existingKafkaExporterCertsChanged || clusterCa.certsRemoved()) {
                                LOGGER.infoCr(reconciliation, "Rolling Kafka Exporter to update or remove certificates");
                                return StrimziClientRollingUpdate();
                            }
                        }

                        // No need to roll, we patched the deployment (and it will roll itself) or we created a new one
                        return Future.succeededFuture();
                    });
        } else  {
            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), StrimziClientResources.deploymentName(reconciliation.name()), null)
                    .map((Void) null);
        }
    }

    /**
     * Triggers the rolling update of the Strimzi Client. This is used to trigger the roll when the certificates change.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> StrimziClientRollingUpdate() {
        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), StrimziClientResources.deploymentName(reconciliation.name()), operationTimeoutMs);
    }

    /**
     * Waits for the Strimzi Client deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    private Future<Void> waitForDeploymentReadiness() {
        if (strimziClient != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), StrimziClientResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), StrimziClientResources.deploymentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
