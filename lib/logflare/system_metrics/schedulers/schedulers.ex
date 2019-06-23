defmodule Logflare.SystemMetrics.Schedulers do
  def scheduler_utilization(sample_a, sample_b) do
    formatter(:scheduler.utilization(sample_a, sample_b))
  end

  defp formatter(scheduler_utilization) do
    Enum.map(scheduler_utilization, fn x ->
      case x do
        {type, scheduler_name, utilization, utilization_percentage} ->
          %{
            name: Integer.to_string(scheduler_name),
            type: Atom.to_string(rename_cpu(type)),
            utilization: Kernel.floor(utilization * 100),
            utilization_percentage: utilization_percentage
          }

        {_total, utilization, utilization_percentage} ->
          %{
            name: "total",
            type: "total",
            utilization: Kernel.floor(utilization * 100),
            utilization_percentage: utilization_percentage
          }
      end
    end)
  end

  defp rename_cpu(:cpu), do: :dirty
  defp rename_cpu(:normal), do: :normal
end
