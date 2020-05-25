#! /bin/bash
export GOOGLE_APPLICATION_CREDENTIALS=/logflare/gcloud_test.json
export LOGFLARE_TEST_USER_WITH_SET_IAM=vitalii.daniuk@gmail.com
export LOGFLARE_TEST_USER_2=vitaliidaniu.k@gmail.com

set -ex

mix ecto.migrate && \
mix run /logflare/test/bq_logs_search_seed.exs && \
mix test