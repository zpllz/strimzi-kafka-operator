#!/usr/bin/env bash
set -e


for i in `ls /tmp/cluster-ca-certs/`
do
  cp -L -r /tmp/cluster-ca-certs/$i  /opt/kafka/cluster-ca-certs/$i.encrypt
done

for i in `ls /tmp/broker-certs/`
do
  cp -L -r /tmp/broker-certs/$i  /opt/kafka/broker-certs/$i.encrypt
done

for i in `ls /tmp/client-ca-certs/`
do
  cp -L -r /tmp/client-ca-certs/$i  /opt/kafka/client-ca-certs/$i.encrypt
done

for i in `ls /tmp/custom-config/`
do
  cp -L -r /tmp/custom-config/$i  /opt/kafka/custom-config/$i.encrypt
done


export JAVA_CLASSPATH=lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.kafka.init.Main
exec "${STRIMZI_HOME}/bin/launch_java.sh"