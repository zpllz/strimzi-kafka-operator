/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Provides Kubernetes secrets containing certificates
 */
public class SecretCertProvider {

    private static final Logger LOGGER = LogManager.getLogger(SecretCertProvider.class);
    public static final String DEFAULT_SECRET_DATA = "qingcloud";
    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @param storeKey key field in the Secret data section for the PKCS12 store
     * @param storePasswordKey key field in the Secret data section for the PKCS12 store password
     * @param storeFile PKCS12 store
     * @param storePassword PKCS12 store password
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     * @throws IOException If a file could not be read.
     */
    public Secret createSecret(String namespace, String name,
                               String keyKey, String certKey,
                               File keyFile, File certFile,
                               String storeKey, String storePasswordKey,
                               File storeFile, String storePassword,
                               Map<String, String> labels, Map<String, String> annotations,
                               OwnerReference ownerReference) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());
        byte[] store = null;
        if (storeFile != null) {
            store = Files.readAllBytes(storeFile.toPath());
        }
        byte[] password = storePassword != null ? storePassword.getBytes(StandardCharsets.US_ASCII) : null;


        return createSecret(namespace, name, keyKey, certKey, key, cert, storeKey, storePasswordKey, store, password, labels, annotations, ownerReference);
    }

    /**
     * Create a Kubernetes secret containing the provided private key and related certificate
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param key private key to store
     * @param cert certificate to store
     * @param storeKey key field in the Secret data section for the PKCS12 store
     * @param storePasswordKey key field in the Secret data section for the PKCS12 store password
     * @param store PKCS12 store
     * @param storePassword PKCS12 store password
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     */
    public Secret createSecret(String namespace, String name,
                               String keyKey, String certKey,
                               byte[] key, byte[] cert,
                               String storeKey, String storePasswordKey,
                               byte[] store, byte[] storePassword,
                               Map<String, String> labels, Map<String, String> annotations,
                               OwnerReference ownerReference) {
        Map<String, String> data = new HashMap<>(4);

        Base64.Encoder encoder = Base64.getEncoder();

        data.put(keyKey, encoder.encodeToString(key));
        data.put(certKey, encoder.encodeToString(cert));
        if (store != null) {
            data.put(storeKey, encoder.encodeToString(store));
        }
        if (storePassword != null) {
            data.put(storePasswordKey, encoder.encodeToString(storePassword));
        }
        return createSecret(namespace, name, data, labels, annotations, ownerReference);
    }

    public static Map<String, String> secretEncryption(Map<String, String> data) {

        Map<String, String> resultMap = new HashMap<>(20);
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String decryptedString = secretEncryptionStr(value);
            String decryptedStringEncode = Base64.getEncoder().encodeToString(decryptedString.getBytes());
            resultMap.put(key, decryptedStringEncode);
            System.out.println("secret cert utils");
            System.out.println(key);
            System.out.println(value);
            System.out.println(decryptedString);
                System.out.println(decryptedStringEncode);

        }
        return resultMap;
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
            LOGGER.error(String.format("run encode secret error %s", a));
        }
        return decryptedStringEncode;
    }


    /**
     * Create a Kubernetes secret containing the provided secret data section
     *
     * @param namespace Namespace
     * @param name Secret name
     * @param data Map with secret data / files
     * @param labels Labels to add to the Secret
     * @param annotations annotations to add to the Secret
     * @param ownerReference owner of the Secret
     * @return the Secret
     */
    public Secret createSecret(String namespace, String name, Map<String, String> data,
                               Map<String, String> labels, Map<String, String> annotations, OwnerReference ownerReference) {
        List<OwnerReference> or = ownerReference != null ? singletonList(ownerReference) : emptyList();
        Map<String, String> Newdata = secretEncryption(data);
        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(or)
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();

        return secret;
    }

    /**
     * Add a data value to an existing Secret
     *
     * @param secret Secret
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param key private key to store
     * @param cert certificate to store
     * @return the Secret
     */
    public Secret addSecret(Secret secret, String keyKey, String certKey, byte[] key, byte[] cert) {
        Base64.Encoder encoder = Base64.getEncoder();
        String keyEncode = encoder.encodeToString(key);
        String certEncode = encoder.encodeToString(cert);

        secret.getData().put(keyKey, secretEncryptionStr(keyEncode));
        secret.getData().put(certKey, secretEncryptionStr(certEncode));

        return secret;
    }

    /**
     * Add a data value to an existing Secret
     *
     * @param secret Secret
     * @param keyKey key field in the Secret data section for the private key
     * @param certKey key field in the Secret data section for the certificate
     * @param keyFile private key to store
     * @param certFile certificate to store
     * @return the Secret
     * @throws IOException If a file could not be read.
     */
    public Secret addSecret(Secret secret, String keyKey, String certKey, File keyFile, File certFile) throws IOException {
        byte[] key = Files.readAllBytes(keyFile.toPath());
        byte[] cert = Files.readAllBytes(certFile.toPath());

        return addSecret(secret, keyKey, certKey, key, cert);
    }
}
