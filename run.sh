#! /bin/sh

# there should always be a secret file
# load the secrets
export $(grep -v '^#' /tmp/.secrets.env | xargs)

if [ "$LIBCLUSTER_TOPOLOGY" == "gce" ] then
    # run gce specific stuff

    sysctl -w net.ipv4.tcp_keepalive_time=60 net.ipv4.tcp_keepalive_intvl=60 net.ipv4.tcp_keepalive_probes=5

    export MY_POD_IP=$(curl \
        -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
        -H "Metadata-Flavor: Google")

fi

./logflare eval Logflare.Release.migrate
./logflare start --sname logflare