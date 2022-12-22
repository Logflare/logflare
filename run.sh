#! /bin/sh

if [ -f .secrets.env ]; then
    while read -r line; do
        export $line
    done < .secrets.env
fi
./logflare eval Logflare.Release.migrate
./logflare start --sname logflare