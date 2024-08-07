#! /bin/sh

# load secrets conditionally
if [ -f /tmp/.secrets.env ]
then
    echo '/tmp/.secrets.env file present, loading secrets...';
    export $(grep -v '^#' /tmp/.secrets.env | xargs);
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
./logflare start --sname logflare