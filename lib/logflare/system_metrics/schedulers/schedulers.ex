defmodule Logflare.SystemMetrics.Schedulers do
  @moduledoc false
  def scheduler_utilization(sample_a, sample_b) do
    formatter(:scheduler.utilization(sample_a, sample_b))
  end

  defp formatter(scheduler_utilization) do
    Enum.map(scheduler_utilization, fn x ->
      case x do
        {type, scheduler_name, utilization, _utilization_percentage} ->
          %{
            name: Integer.to_string(scheduler_name),
            type: rename_type(type),
            utilization: Kernel.floor(utilization * 100),
            utilization_percentage: utilization * 100
          }

        {_total, utilization, _utilization_percentage} ->
          %{
            name: "total",
            type: "total",
            utilization: Kernel.floor(utilization * 100),
            utilization_percentage: utilization * 100
          }
      end
    end)
  end

  defp rename_type(:cpu), do: "dirty"
  defp rename_type(:normal), do: "normal"
end
