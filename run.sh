#! /bin/sh

if [ -f .secrets.env ]; then
    export $(cat .secrets.env | xargs)
end

./logflare eval Logflare.Release.migrate
./logflare start --sname logflare