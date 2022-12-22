#! /bin/sh
if [ -f /tmp/.secrets.env ]; then
    while read -r line; do
        export $line > /dev/null
    done < /tmp/.secrets.env
fi

./logflare eval Logflare.Release.migrate
./logflare start --sname logflare