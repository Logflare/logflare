#! /bin/sh
echo "Migrating..."
./logflare eval Logflare.Release.migrate
echo "Migration done; Starting app"
./logflare start --sname logflare