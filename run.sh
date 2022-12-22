#! /bin/sh

if [ -f .staging.env ]; then
    while read -r line; do
        export $line
    done < .staging.env
fi
./logflare eval Logflare.Release.migrate
./logflare start --sname logflare