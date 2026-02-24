#!/bin/bash  

# echo $PWD

NODE_IP=$(curl \
    -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" \
    -H "Metadata-Flavor: Google")

NODE=logflare@$NODE_IP

echo "Stopping node $NODE"

curl -X "POST" "https://api.logflarestaging.com/api/logs?source=$LOGFLARE_LOGGER_BACKEND_SOURCE_ID" \
    -H 'Content-Type: application/json' \
    -H "X-API-KEY: $LOGFLARE_LOGGER_BACKEND_API_KEY" \
    -d $"{
      \"message\": \"Stopping node $NODE\"
    }"

curl -X "PUT" "http://localhost:4000/admin/shutdown?code=$LOGFLARE_NODE_SHUTDOWN_CODE"

sleep 20

echo "Stopped node $NODE"