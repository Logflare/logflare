defmodule Logflare.SystemMetrics.Observer do
  # def observer_metrics() do
  #   :observer_backend.sys_info()
  #   |> Keyword.drop([:alloc_info])
  # end

  def get_memory() do
    :erlang.memory() |> Enum.map(fn {k, v} -> {k, div(v, 1024 * 1024)} end) |> Enum.into(%{})
  end

  def get_metrics() do
    {{:input, input}, {:output, output}} = :erlang.statistics(:io)

    [
      uptime: :erlang.statistics(:wall_clock) |> elem(0),
      run_queue: :erlang.statistics(:total_run_queue_lengths_all),
      io_input: input,
      io_output: output,
      logical_processors: :erlang.system_info(:logical_processors),
      logical_processors_online: :erlang.system_info(:logical_processors_online),
      logical_processors_available: :erlang.system_info(:logical_processors_available),
      schedulers: :erlang.system_info(:schedulers),
      schedulers_online: :erlang.system_info(:schedulers_online),
      schedulers_available: :erlang.system_info(:schedulers_online),
      otp_release: :erlang.system_info(:otp_release),
      version: :erlang.system_info(:version),
      system_architecture: nil,
      kernel_poll: nil,
      smp_support: nil,
      threads: nil,
      thread_pool_size: nil,
      wordsize_internal: nil,
      wordsize_external: nil,
      atom_limit: :erlang.system_info(:atom_limit),
      atom_count: :erlang.system_info(:atom_count),
      process_limit: :erlang.system_info(:process_limit),
      process_count: :erlang.system_info(:process_count),
      port_limit: :erlang.system_info(:port_limit),
      port_count: :erlang.system_info(:port_count),
      ets_limit: :erlang.system_info(:ets_limit),
      ets_count: :erlang.system_info(:ets_count),
      dist_buf_busy_limit: nil,
      # Memory metrics are in the memory poller
      total: nil,
      processes: nil,
      processes_used: nil,
      system: nil,
      atom: nil,
      atom_used: nil,
      binary: nil,
      code: nil,
      ets: nil
    ]
  end
end
