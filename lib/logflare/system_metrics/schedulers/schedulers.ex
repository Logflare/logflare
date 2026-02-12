defmodule Logflare.SystemMetrics.Schedulers do
  @moduledoc false

  def async_dispatch_stats(duration \\ to_timeout(second: 1)) do
    Logflare.Utils.Tasks.start_child(fn -> collect_dispatch_stats(duration) end)
  end

  defp collect_dispatch_stats(duration \\ to_timeout(second: 1)) do
    :erlang.system_flag(:scheduler_wall_time, true)
    prev_sample = :scheduler.get_sample_all()
    Process.sleep(duration)
    next_sample = :scheduler.get_sample_all()
    :erlang.system_flag(:scheduler_wall_time, false)

    utilization = :scheduler.utilization(prev_sample, next_sample)

    Enum.each(utilization, fn x ->
      case x do
        {type, id, util, _pct} ->
          :telemetry.execute(
            [:logflare, :system, :scheduler, :utilization],
            %{utilization: Kernel.floor(util * 100)},
            %{name: Integer.to_string(id), type: rename_type(type)}
          )

        {:total, util, _pct} ->
          :telemetry.execute(
            [:logflare, :system, :scheduler, :utilization],
            %{utilization: Kernel.floor(util * 100)},
            %{name: "total", type: "total"}
          )

        {:weighted, util, _pct} ->
          :telemetry.execute(
            [:logflare, :system, :scheduler, :utilization],
            %{utilization: Kernel.floor(util * 100)},
            %{name: "weighted", type: "weighted"}
          )
      end
    end)
  end

  defp rename_type(:cpu), do: "dirty"
  defp rename_type(:io), do: "dirty (io)"
  defp rename_type(:normal), do: "normal"
end
