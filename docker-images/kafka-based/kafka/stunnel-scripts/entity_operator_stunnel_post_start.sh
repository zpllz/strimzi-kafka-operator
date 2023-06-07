#!/usr/bin/env bash
set -e

sleep 60
rm -rf /etc/eto-certs/*.key
rm -rf /etc/eto-certs/*.password
rm -rf /etc/tls-sidecar/cluster-ca-certs/*.password