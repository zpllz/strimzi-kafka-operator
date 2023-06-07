#!/usr/bin/env bash
set -e

for i in `ls /tmp/euo-certs/`
do
  cp -L  /tmp/euo-certs/$i  /etc/euo-certs/$i.encrypt
  chmod 777   /etc/euo-certs/$i.encrypt
done

for i in `ls /tmp/cluster-ca-certs/`
do
  cp -L /tmp/cluster-ca-certs/$i /etc/tls-sidecar/cluster-ca-certs-topic/$i.encrypt
  chmod 777  /etc/tls-sidecar/cluster-ca-certs-topic/$i.encrypt
  cp -L /tmp/cluster-ca-certs/$i /etc/tls-sidecar/cluster-ca-certs-tls/$i.encrypt
  chmod 777  /etc/tls-sidecar/cluster-ca-certs-tls/$i.encrypt
  cp -L /tmp/cluster-ca-certs/$i /etc/tls-sidecar/cluster-ca-certs-user/$i.encrypt
  chmod 777  /etc/tls-sidecar/cluster-ca-certs-user/$i.encrypt
done

for i in `ls /tmp/eto-certs/`
do
  cp -L  /tmp/eto-certs/$i  /etc/eto-certs/$i.encrypt
  chmod 777  /etc/eto-certs/$i.encrypt
  cp -L  /tmp/eto-certs/$i  /etc/eto-certs-tls/$i.encrypt
  chmod 777  /etc/eto-certs-tls/$i.encrypt
done

cp -L -r /tmp/user-operator/custom-config/*  /opt/user-operator/custom-config
chmod 777  /opt/user-operator/custom-config/*
cp -L -r /tmp/topic-operator/custom-config/* /opt/topic-operator/custom-config/
chmod 777  /opt/topic-operator/custom-config/*
