#!/usr/bin/env bash
set -e

for i in `ls /tmp/zookeeper-node-certs/`
do
  cp -L  /tmp/zookeeper-node-certs/$i  /opt/kafka/zookeeper-node-certs/$i.encrypt
done

for i in `ls /tmp/cluster-ca-certs/`
do
  cp -L /tmp/cluster-ca-certs/$i /opt/kafka/cluster-ca-certs/$i.encrypt
done

cp -L -r /tmp/custom-config/*  /opt/kafka/custom-config/
