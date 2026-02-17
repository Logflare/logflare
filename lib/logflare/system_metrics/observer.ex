defmodule Logflare.SystemMetrics.Observer do
  @moduledoc false

  def dispatch_stats do
    metrics = get_metrics()
    memory = get_memory()

    :telemetry.execute([:logflare, :system, :observer, :metrics], metrics)
    :telemetry.execute([:logflare, :system, :observer, :memory], memory)
  end

  defp get_memory do
    %{memory: persistent_term_memory} = :persistent_term.info()
    memories = [{:persistent_term, persistent_term_memory} | :erlang.memory()]
    Map.new(memories)
  end

  defp get_metrics do
    {{:input, input}, {:output, output}} = :erlang.statistics(:io)
    {uptime, _} = :erlang.statistics(:wall_clock)

    %{
      uptime: uptime,
      run_queue: :erlang.statistics(:total_run_queue_lengths_all),
      io_input: input,
      io_output: output,
      logical_processors: to_integer(:erlang.system_info(:logical_processors)),
      logical_processors_online: to_integer(:erlang.system_info(:logical_processors_online)),
      logical_processors_available: to_integer(:erlang.system_info(:logical_processors_available)),
      schedulers: :erlang.system_info(:schedulers),
      schedulers_online: :erlang.system_info(:schedulers_online),
      otp_release: :erlang.system_info(:otp_release) |> List.to_integer(),
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

  defp to_integer(val) when is_integer(val), do: val
  defp to_integer(_), do: 0
end
