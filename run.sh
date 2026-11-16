#! /bin/sh

# load secrets conditionally
if [ -f /tmp/.secrets.env ]
then
    echo '/tmp/.secrets.env file present, loading secrets...';
    export $(grep -v '^#' /tmp/.secrets.env | grep -v '^$' | while IFS= read -r line; do
        var_name=$(echo "$line" | cut -d'=' -f1)
        if [ -z "$(eval echo \$$var_name)" ]; then
            echo "$line"
        fi
    done | xargs);
fi

# maybe run a startup script
if [ -f ./startup.sh ]
then
    echo 'startup.sh file present, sourcing...';
    sleep .5;
    . ./startup.sh;
fi

echo "LOGFLARE_NODE_HOST is: $LOGFLARE_NODE_HOST"

./logflare eval Logflare.Release.migrate
# exec so the release replaces this shell: Docker and Kubernetes only signal the
# container's main process, and a shell left in between never passes SIGTERM on.
exec ./logflare start --sname logflare