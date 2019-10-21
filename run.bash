#! /bin/bash
# /logflare/_build/staging/rel/logflare/bin/logflare eval "Logflare.Tasks.ReleaseTasks.setup()" && \
export GOOGLE_APPLICATION_CREDENTIALS=/logflare/gcloud.json

set -ex

export MY_POD_IP=$(curl \
    -s "http://metadata.google.internal/computeMetadata/v1/instance/hostname" \
    -H "Metadata-Flavor: Google")

mix ecto.migrate && \
/logflare/_build/prod/rel/logflare/bin/logflare start
