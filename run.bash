#! /bin/bash
# /logflare/_build/staging/rel/logflare/bin/logflare eval "Logflare.Tasks.ReleaseTasks.setup()" && \
mix ecto.migrate && \
/logflare/_build/staging/rel/logflare/bin/logflare start
