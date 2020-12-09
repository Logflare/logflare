#! /bin/bash
# /logflare/_build/staging/rel/logflare/bin/logflare eval "Logflare.Tasks.ReleaseTasks.setup()" && \
export GOOGLE_APPLICATION_CREDENTIALS=/logflare/gcloud.json

set -ex

sysctl -w net.ipv4.tcp_keepalive_time=60 net.ipv4.tcp_keepalive_intvl=60 net.ipv4.tcp_keepalive_probes=5

export MY_POD_IP=$(curl \
    -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
    -H "Metadata-Flavor: Google")

./cloud_sql_proxy -instances=logflare-232118:us-central1:logflare-prod-20201208-global=tcp:5432 &

sleep 10

mix ecto.migrate && \
/logflare/_build/prod/rel/logflare/bin/logflare start
