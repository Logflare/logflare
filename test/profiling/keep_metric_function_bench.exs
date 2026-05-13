# Benchmark script for Logflare.Backends.UserMonitoring.keep_metric_function/1
#
# Run with: elixir test/profiling/keep_metric_function_bench.exs
#
# This function is called by Telemetry.Metrics for each metric to determine
# whether to keep or discard metrics based on user monitoring settings.

alias Logflare.Backends.UserMonitoring
alias Logflare.Logs.Processor
alias Logflare.Sources
alias Logflare.Users

import Logflare.Factory

# Setup Mimic to stub Processor.ingest to avoid actual ingestion
Mimic.copy(Logflare.Logs.Processor)
Mimic.stub(Processor, :ingest, fn _, _, _ -> :ok end)

# Create users with different monitoring states
user_with_monitoring = insert(:user, system_monitoring: true)
user_without_monitoring = insert(:user, system_monitoring: false)

# Create sources for the users
source_monitored = insert(:source, user: user_with_monitoring)
source_not_monitored = insert(:source, user: user_without_monitoring)

# Create system sources for the monitored user
_logs_system_source =
  insert(:source,
    user: user_with_monitoring,
    system_source: true,
    system_source_type: :logs
  )

_metrics_system_source =
  insert(:source,
    user: user_with_monitoring,
    system_source: true,
    system_source_type: :metrics
  )

# Create a backend and endpoint for testing all lookup paths
backend = insert(:backend, user: user_with_monitoring)
endpoint = insert(:endpoint, user: user_with_monitoring)

# Warm the caches
Users.Cache.get(user_with_monitoring.id)
Users.Cache.get(user_without_monitoring.id)
Sources.Cache.get_by_id(source_monitored.id)
Sources.Cache.get_by_id(source_not_monitored.id)
Sources.Cache.get_source_by_token(source_monitored.token)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :logs)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :metrics)
Logflare.Backends.Cache.get_backend(backend.id)
Logflare.Endpoints.Cache.get_endpoint_query(endpoint.id)

# ============================================================================
# Build test inputs
# ============================================================================

keep_metric_user_id_true = %{"user_id" => user_with_monitoring.id}
keep_metric_user_id_false = %{"user_id" => user_without_monitoring.id}
keep_metric_source_id_true = %{"source_id" => source_monitored.id}
keep_metric_source_id_false = %{"source_id" => source_not_monitored.id}
keep_metric_source_token = %{"source_token" => source_monitored.token}
keep_metric_backend_id = %{"backend_id" => backend.id}
keep_metric_endpoint_id = %{"endpoint_id" => endpoint.id}
keep_metric_no_user = %{"unrelated_key" => "value"}

# ============================================================================
# Benchmark: keep_metric_function/1
# ============================================================================

Benchee.run(
  %{
    "keep_metric_function" => fn input ->
      UserMonitoring.keep_metric_function(input)
    end
  },
  inputs: %{
    "user_id (monitoring=true)" => keep_metric_user_id_true,
    "user_id (monitoring=false)" => keep_metric_user_id_false,
    "source_id (monitoring=true)" => keep_metric_source_id_true,
    "source_id (monitoring=false)" => keep_metric_source_id_false,
    "source_token" => keep_metric_source_token,
    "backend_id" => keep_metric_backend_id,
    "endpoint_id" => keep_metric_endpoint_id,
    "no user (early return)" => keep_metric_no_user
  },
  time: 3,
  warmup: 1
)
