#!/usr/bin/env bash
set -e
set +x

for i in `ls /etc/eto-certs/ | grep ".encrypt"`
do
  decrypt  /etc/eto-certs/$i /etc/eto-certs/${i%.encrypt*} decode
done

for i in  `ls /etc/tls-sidecar/cluster-ca-certs/ | grep ".encrypt"`
do
  decrypt  /etc/tls-sidecar/cluster-ca-certs/$i /etc/tls-sidecar/cluster-ca-certs/${i%.encrypt*} decode
done

# Generate and print the config file
echo "Starting Stunnel with configuration:"
"${STUNNEL_HOME}"/entity_operator_stunnel_config_generator.sh | tee /tmp/stunnel.conf
echo ""

set -x

# starting Stunnel with final configuration
exec /usr/bin/tini -w -e 143 -- /usr/bin/stunnel /tmp/stunnel.conf
