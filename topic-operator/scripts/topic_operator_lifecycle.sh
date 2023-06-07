#!/usr/bin/env bash
set -e

sleep 60
a=1
b=0
while [ 200 -ne `curl -I -m 5 -s -w "%{http_code}\n" -o /dev/null  localhost:8080/ready` ]
do
  if [ $a -eq 10 ]
  then
    break
  fi
  sleep 2
  b=1
  a=$[$a+1]
done
if [ $b -eq 0 ]
then
  rm -f /etc/eto-certs/*.key
  rm -f /etc/tls-sidecar/cluster-ca-certs/*.password
  rm -f /etc/eto-certs/*.password
fi
