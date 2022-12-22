#! /bin/sh

if [ -f .secrets.env ]; then
    export $(xargs < .secrets.env)
fi

./logflare eval Logflare.Release.migrate
./logflare start --sname logflare