#! /bin/sh

# load secrets conditionally
if [ -f /tmp/.secrets.env ]
    then
    echo '/tmp/.secrets.env file present, loading secrets...'; 
    export $(grep -v '^#' /tmp/.secrets.env | xargs);
fi


if [[ "$LIBCLUSTER_TOPOLOGY" == "gce" ]]
then
    # run gce specific stuff

    sysctl -w net.ipv4.tcp_keepalive_time=60 net.ipv4.tcp_keepalive_intvl=60 net.ipv4.tcp_keepalive_probes=5

    export LOGFLARE_NODE_HOST=$(curl \
        -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
        -H "Metadata-Flavor: Google")

fi

./logflare eval Logflare.Release.migrate
./logflare start --sname logflare