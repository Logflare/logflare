# Benchmark script for Logflare.Backends.UserMonitoring.log_interceptor/2
#
# Run with: elixir test/profiling/log_interceptor_bench.exs
#
# This function is called by Logger for every log message to determine
# whether to intercept and forward logs based on user monitoring settings.

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

# Warm the caches
Users.Cache.get(user_with_monitoring.id)
Users.Cache.get(user_without_monitoring.id)
Sources.Cache.get_by_id(source_monitored.id)
Sources.Cache.get_by_id(source_not_monitored.id)
Sources.Cache.get_source_by_token(source_monitored.token)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :logs)
Sources.Cache.get_by(user_id: user_with_monitoring.id, system_source_type: :metrics)

# ============================================================================
# Build test inputs
# ============================================================================

base_log_event = %{
  level: :info,
  msg: {:string, "test log message"},
  meta: %{
    pid: self(),
    gl: self(),
    time: System.system_time(:microsecond)
  }
}

log_event_full_path = put_in(base_log_event, [:meta, :source_id], source_monitored.id)
log_event_no_user = base_log_event
log_event_monitoring_false = put_in(base_log_event, [:meta, :source_id], source_not_monitored.id)
log_event_no_meta = Map.delete(base_log_event, :meta)

# ============================================================================
# Benchmark: log_interceptor/2
# ============================================================================

Benchee.run(
  %{
    "log_interceptor" => fn input ->
      UserMonitoring.log_interceptor(input, [])
    end
  },
  inputs: %{
    "full path (ingests)" => log_event_full_path,
    "early return (no user_id)" => log_event_no_user,
    "early return (monitoring=false)" => log_event_monitoring_false,
    "early return (no meta)" => log_event_no_meta
  },
  time: 3,
  warmup: 1
)
