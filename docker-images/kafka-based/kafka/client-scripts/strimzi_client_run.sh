#!/usr/bin/env bash
set -e
set +x

if [[ $STRIMZI_CLIENT_AUTHENTICATION_TYPE == "tls" && $STRIMZI_CLIENT_USE_TLS == "true" ]]; then
tls_password=`cat /etc/strimzi-client/cluster-ca-certs/ca.password`
user_password=`cat /etc/strimzi-client/strimzi-client-user-certs/user.password`
cat << EOF > /opt/data/client.properties
security.protocol=ssl
ssl.truststore.location=/etc/strimzi-client/cluster-ca-certs/ca.p12
ssl.truststore.password=$tls_password
ssl.truststore.type=pkcs12

ssl.keystore.location=/etc/strimzi-client/strimzi-client-user-certs/user.p12
ssl.keystore.password=$user_password
ssl.keystore.type=pkcs12
ssl.endpoint.identification.algorithm=
EOF

elif [[ $STRIMZI_CLIENT_AUTHENTICATION_TYPE == "scram-sha-512" && $STRIMZI_CLIENT_USE_TLS == "true" ]]; then

jaas_config=`cat /etc/strimzi-client/strimzi-client-user-certs/sasl.jaas.config`
cat << EOF > /opt/kafka/config/jaas.conf
KafkaClient {
  $jaas_config
};
EOF

tls_password=`cat /etc/strimzi-client/cluster-ca-certs/ca.password`
cat << EOF > /opt/data/client.properties
security.protocol=sasl_ssl
sasl.mechanism=SCRAM-SHA-512
ssl.truststore.location=/etc/strimzi-client/cluster-ca-certs/ca.p12
ssl.truststore.password=$tls_password
ssl.truststore.type=pkcs12
ssl.endpoint.identification.algorithm=
EOF

elif [[ $STRIMZI_CLIENT_AUTHENTICATION_TYPE == "scram-sha-512" && $STRIMZI_CLIENT_USE_TLS == "false" ]]; then

jaas_config=`cat /etc/strimzi-client/strimzi-client-user-certs/sasl.jaas.config`
cat << EOF > /opt/kafka/config/jaas.conf
KafkaClient {
  $jaas_config
};
EOF

cat << EOF > /opt/data/client.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
ssl.endpoint.identification.algorithm=
EOF


elif [[ $STRIMZI_CLIENT_USE_TLS == "true" ]]; then
tls_password=`cat /etc/strimzi-client/cluster-ca-certs/ca.password`
cat << EOF > /opt/data/client.properties
security.protocol=SSL
ssl.truststore.location=/etc/strimzi-client/cluster-ca-certs/ca.p12
ssl.truststore.password=$tls_password
ssl.truststore.type=pkcs12
ssl.endpoint.identification.algorithm=
EOF

else
cat << EOF > /opt/data/client.properties
EOF

fi


# starting Kafka Exporter with final configuration
cat <<EOT > /tmp/run.sh
while true;do sleep 1000;done
EOT

chmod +x /tmp/run.sh

set -x

exec /usr/bin/tini -w -e 143 -- /tmp/run.sh
