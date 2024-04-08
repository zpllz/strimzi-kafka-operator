#!/usr/bin/env bash
set -e
set +x

# starting Kafka Exporter with final configuration
cat <<EOT > /tmp/run.sh
while true;do sleep 1000;done
EOT

chmod +x /tmp/run.sh

set -x

exec /usr/bin/tini -w -e 143 -- /tmp/run.sh
