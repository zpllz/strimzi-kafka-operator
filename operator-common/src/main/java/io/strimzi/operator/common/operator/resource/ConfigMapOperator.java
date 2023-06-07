/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Operations for {@code ConfigMap}s.
 */
public class ConfigMapOperator extends AbstractResourceOperator<KubernetesClient, ConfigMap, ConfigMapList, Resource<ConfigMap>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ConfigMapOperator.class);

    /**
     * Constructor
     *
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ConfigMapOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ConfigMap");
    }

    @Override
    protected MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> operation() {
        return client.configMaps();
    }

    @Override
    protected Future<ReconcileResult<ConfigMap>> internalPatch(Reconciliation reconciliation, String namespace, String name, ConfigMap current, ConfigMap desired) {
        try {
            if (compareObjects(current.getData(), desired.getData())
                    && compareObjects(current.getMetadata().getName(), desired.getMetadata().getName())
                    && compareObjects(current.getMetadata().getNamespace(), desired.getMetadata().getNamespace())
                    && compareObjects(current.getMetadata().getAnnotations(), desired.getMetadata().getAnnotations())
                    && compareObjects(current.getMetadata().getLabels(), desired.getMetadata().getLabels())) {
                // Checking some metadata. We cannot check entire metadata object because it contains
                // timestamps which would cause restarting loop
                LOGGER.debugCr(reconciliation, "{} {} in namespace {} has not been patched because resources are equal", resourceKind, name, namespace);
                return Future.succeededFuture(ReconcileResult.noop(current));
            } else {
                return super.internalPatch(reconciliation, namespace, name, current, desired);
            }
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return Future.failedFuture(e);
        }
    }

    @Override
    public Future<ReconcileResult<ConfigMap>> reconcile(Reconciliation reconciliation, String namespace, String name, ConfigMap desired) {
        if (desired != null && !namespace.equals(desired.getMetadata().getNamespace())) {
            return Future.failedFuture("Given namespace " + namespace + " incompatible with desired namespace " + desired.getMetadata().getNamespace());
        } else if (desired != null && !name.equals(desired.getMetadata().getName())) {
            return Future.failedFuture("Given name " + name + " incompatible with desired name " + desired.getMetadata().getName());
        }

        Promise<ReconcileResult<ConfigMap>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    ConfigMap current = operation().inNamespace(namespace).withName(name).get();
                    if (desired != null) {
                        ConfigMap desiredencryption = secretEncryption(desired);
                        if (current == null) {
                            LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, creating it", resourceKind, namespace, name);
                            internalCreate(reconciliation, namespace, name, desired).onComplete(future);
                            internalCreate(reconciliation, namespace, name + "-encrypt", desiredencryption).onComplete(future);
                        } else {
                            LOGGER.debugCr(reconciliation, "{} {}/{} already exists, patching it", resourceKind, namespace, name);
                            internalPatch(reconciliation, namespace, name, current, desired).onComplete(future);
                            internalPatch(reconciliation, namespace, name + "-encrypt", current, desiredencryption).onComplete(future);
                        }
                    } else {
                        if (current != null) {
                            // Deletion is desired
                            LOGGER.debugCr(reconciliation, "{} {}/{} exist, deleting it", resourceKind, namespace, name);
                            internalDelete(reconciliation, namespace, name).onComplete(future);
                            internalDelete(reconciliation, namespace, name + "-encrypt").onComplete(future);
                        } else {
                            LOGGER.debugCr(reconciliation, "{} {}/{} does not exist, noop", resourceKind, namespace, name);
                            future.complete(ReconcileResult.noop(null));
                        }
                    }

                },
                false,
                promise
        );
        return promise.future();
    }

    public static ConfigMap secretEncryption(ConfigMap configMap) {

        ObjectMetaBuilder metadata = new ObjectMetaBuilder()
                .withName(configMap.getMetadata().getName() + "-encrypt")
                .withNamespace(configMap.getMetadata().getNamespace());

        if (configMap.getMetadata().getOwnerReferences() != null ) {
            metadata.withOwnerReferences(configMap.getMetadata().getOwnerReferences());
        }
        if (configMap.getMetadata().getLabels() != null ) {
            metadata.withLabels(configMap.getMetadata().getLabels());
        }
        if (configMap.getMetadata().getAnnotations() != null ) {
            metadata.withAnnotations(configMap.getMetadata().getAnnotations());
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> entry : configMap.getData().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String decryptedString = secretEncryptionStr(value);
            System.out.println("configmap encrypt code");
            System.out.println(key);
            System.out.println(value);
            System.out.println(decryptedString);
            resultMap.put(key, decryptedString);
        }
        return new ConfigMapBuilder()
                .withMetadata(metadata.build())
                .withData(resultMap).build();
    }

    private boolean compareObjects(Object a, Object b) {
        if (a == null && b instanceof Map && ((Map) b).size() == 0)
            return true;
        return !(a instanceof Map ^ b instanceof Map) && Objects.equals(a, b);
    }
}
