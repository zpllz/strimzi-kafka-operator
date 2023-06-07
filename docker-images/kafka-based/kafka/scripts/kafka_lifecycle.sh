#!/usr/bin/env bash
set -e


sleep 60
rm -rf  /opt/kafka/broker-certs/*.key
rm -rf /opt/kafka/cluster-ca-certs/*.key
rm -rf /opt/kafka/client-ca-certs/*.key
rm -rf /opt/kafka/cluster-ca-certs/*.password
rm -rf /opt/kafka/client-ca-certs/*.password
rm -rf /opt/kafka/broker-certs/*.password
rm -rf /opt/kafka/custom-config/server.config
rm -rf /tmp/*properties*

