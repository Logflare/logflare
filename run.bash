#! /bin/bash
# /logflare/_build/staging/rel/logflare/bin/logflare eval "Logflare.Tasks.ReleaseTasks.setup()" && \
export GOOGLE_APPLICATION_CREDENTIALS=/logflare/gcloud.json

mix ecto.migrate && \
/logflare/_build/prod/rel/logflare/bin/logflare start
