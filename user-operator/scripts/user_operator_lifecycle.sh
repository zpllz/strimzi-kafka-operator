#!/usr/bin/env bash
set -e

sleep 60
rm -f /etc/euo-certs/*.key
rm -f /etc/euo-certs/*.password
rm -f /etc/tls-sidecar/cluster-ca-certs/*.password
