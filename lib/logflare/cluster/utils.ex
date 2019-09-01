defmodule Logflare.Cluster.Utils do
  def node_list_all() do
    [Node.self() | Node.list()]
  end

  def merge_metadata(list) do
    {_, data} =
      Enum.reduce(list, {:noop, %{}}, fn {_, y}, {_, acc} ->
        y
        |> Map.update(:average_rate, 0, &(&1 + (acc[:average_rate] || 0)))
        |> Map.update(
          :max_rate,
          0,
          &if(&1 < (acc[:max_rate] || 0), do: &1, else: acc[:max_rate] || 0)
        )
        |> Map.update(:last_rate, 0, &(&1 + (acc[:last_rate] || 0)))

        {:noop, y}
      end)

    data
  end
end
