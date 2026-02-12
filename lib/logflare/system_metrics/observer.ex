defmodule Logflare.SystemMetrics.Observer do
  @moduledoc false

  def dispatch_stats do
    observer_metrics = get_metrics()
    mem_metrics = get_memory()

    :telemetry.execute([:logflare, :system, :observer, :metrics], observer_metrics)
    :telemetry.execute([:logflare, :system, :observer, :memory], mem_metrics)
  end

  defp get_memory do
    %{memory: persistent_term_memory} = :persistent_term.info()
    memory = [{:persistent_term, persistent_term_memory} | :erlang.memory()]
    Map.new(memory, fn {k, v} -> {k, div(v, 1024 * 1024)} end)
  end

  defp get_metrics do
    {{:input, input}, {:output, output}} = :erlang.statistics(:io)
    {uptime, _} = :erlang.statistics(:wall_clock)

    %{
      uptime: uptime,
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
      atom_limit: :erlang.system_info(:atom_limit),
      atom_count: :erlang.system_info(:atom_count),
      process_limit: :erlang.system_info(:process_limit),
      process_count: :erlang.system_info(:process_count),
      port_limit: :erlang.system_info(:port_limit),
      port_count: :erlang.system_info(:port_count),
      ets_limit: :erlang.system_info(:ets_limit),
      ets_count: :erlang.system_info(:ets_count),
      total_active_tasks: :erlang.statistics(:total_active_tasks)
    }
  end
end
