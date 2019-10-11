use Mix.Config

config :telemetry_poller, :default,
  vm_measurements: [:memory, :total_run_queue_lengths],
  period: 1_000
