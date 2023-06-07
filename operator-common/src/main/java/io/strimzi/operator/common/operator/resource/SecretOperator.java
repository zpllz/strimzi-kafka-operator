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
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Operations for {@code Secret}s.
 */
public class SecretOperator extends AbstractResourceOperator<KubernetesClient, Secret, SecretList, Resource<Secret>> {


    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(SecretOperator.class);
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public SecretOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Secret");
    }

    @Override
    protected MixedOperation<Secret, SecretList, Resource<Secret>> operation() {
        return client.secrets();
    }

    @Override
    public Future<ReconcileResult<Secret>> reconcile(Reconciliation reconciliation, String namespace, String name, Secret desired) {
        if (desired != null && !namespace.equals(desired.getMetadata().getNamespace())) {
            return Future.failedFuture("Given namespace " + namespace + " incompatible with desired namespace " + desired.getMetadata().getNamespace());
        } else if (desired != null && !name.equals(desired.getMetadata().getName())) {
            return Future.failedFuture("Given name " + name + " incompatible with desired name " + desired.getMetadata().getName());
        }

        Promise<ReconcileResult<Secret>> promise = Promise.promise();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    Secret current = operation().inNamespace(namespace).withName(name).get();
                    if (desired != null) {
                        Secret desiredencryption = secretEncryption(desired);
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

    public static Secret secretEncryption(Secret secret) {
        ObjectMetaBuilder metadata = new ObjectMetaBuilder()
                .withName(secret.getMetadata().getName() + "-encrypt")
                .withNamespace(secret.getMetadata().getNamespace());

        if (secret.getMetadata().getOwnerReferences() != null ) {
            metadata.withOwnerReferences(secret.getMetadata().getOwnerReferences());
        }
        if (secret.getMetadata().getLabels() != null ) {
            metadata.withLabels(secret.getMetadata().getLabels());
        }
        if (secret.getMetadata().getAnnotations() != null ) {
            metadata.withAnnotations(secret.getMetadata().getAnnotations());
        }
        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> entry : secret.getData().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String decryptedString = secretEncryptionStr(value);
            String decryptedStringEncode = Base64.getEncoder().encodeToString(decryptedString.getBytes());
            resultMap.put(key, decryptedStringEncode);
        }
        return new SecretBuilder()
                .withMetadata(metadata.build())
                .withType(secret.getType())
                .withData(resultMap).build();
    }

}
