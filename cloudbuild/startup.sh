#! /bin/sh

# wait for networking to be ready before starting Erlang
echo 'Sleeping for 15 seconds for GCE networking to be ready...'
sleep 15

# add in monitoring cpu timer interrupts
sysctl -w net.ipv4.tcp_keepalive_time=60 net.ipv4.tcp_keepalive_intvl=60 net.ipv4.tcp_keepalive_probes=5 kernel.nmi_watchdog=1

LOGFLARE_NODE_HOST=$(curl \
    -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
    -H "Metadata-Flavor: Google")

# append to secrets file
echo "$LOGFLARE_NODE_HOST" >> /tmp/.secrets.env