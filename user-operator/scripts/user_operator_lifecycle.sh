#!/usr/bin/env bash
set -e

sleep 60
while true
do
  if [ -f /etc/euo-certs/entity-operator.key ]
  then
    rm -rf /etc/euo-certs/*.key
    break
  fi
  sleep 5
done

while true
do
  if [ -f /etc/euo-certs/entity-operator.password ]
  then
    rm -rf /etc/euo-certs/*.password
    break
  fi
  sleep 5
done


while true
do
  if [  -f /etc/tls-sidecar/cluster-ca-certs/ca.password ]
  then
    rm -rf /etc/tls-sidecar/cluster-ca-certs/*.password
    break
  fi
  sleep 5
done

