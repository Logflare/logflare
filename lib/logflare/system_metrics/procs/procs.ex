defmodule Logflare.SystemMetrics.Procs do
  def get_processes() do
    process_list =
      for {{:registered_name, name}, {:reductions, reds}, {:pid, pid}} <-
            Stream.map(
              Process.list(),
              &{Process.info(&1, :registered_name), Process.info(&1, :reductions), {:pid, &1}}
            ),
          into: %{},
          do: {name || pid, reds}

    total_reds = process_list |> Stream.map(fn {_k, v} -> v end) |> Enum.sum()

    %{process_list: process_list, total_reductions: total_reds}
  end
end
