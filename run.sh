#! /bin/sh

# load secrets conditionally
if [ -f /tmp/.secrets.env ]
then
    echo '/tmp/.secrets.env file present, loading secrets...';
    export $(grep -v '^#' /tmp/.secrets.env | xargs);
fi

if [[ "$LIBCLUSTER_TOPOLOGY" == "postgres" ]]
then
    # run gce specific stuff

    # wait for networking to be ready before starting Erlang
    echo 'Sleeping for 15 seconds for GCE networking to be ready...'
    sleep 15

    sysctl -w net.ipv4.tcp_keepalive_time=60 net.ipv4.tcp_keepalive_intvl=60 net.ipv4.tcp_keepalive_probes=5

    export LOGFLARE_NODE_HOST=$(curl \
        -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
        -H "Metadata-Flavor: Google")

fi

if [[ "$OVERRIDE_MAGIC_COOKIE" ]]
then 
    echo "OVERRIDE_MAGIC_COOKIE is set, using it..."
    export MAGIC_COOKIE=$OVERRIDE_MAGIC_COOKIE
    echo $MAGIC_COOKIE > /tmp/.magic_cookie
fi

echo "LOGFLARE_NODE_HOST is: $LOGFLARE_NODE_HOST"

./logflare eval Logflare.Release.migrate
RELEASE_COOKIE=$(cat /tmp/.magic_cookie 2>/dev/null || echo $RANDOM | md5sum | head -c 20) ./logflare start --sname logflare