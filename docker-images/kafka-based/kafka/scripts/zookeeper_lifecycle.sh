#!/usr/bin/env bash
set -e

sleep 60
rm -rf  /opt/kafka/custom-config/*
rm -rf /opt/kafka/zookeeper-node-certs/*.key
rm -rf /opt/kafka/cluster-ca-certs/*.key
rm -rf  /opt/kafka/cluster-ca-certs/*.password
rm -rf /opt/kafka/zookeeper-node-certs/*.password
rm -rf /tmp/*properties*
