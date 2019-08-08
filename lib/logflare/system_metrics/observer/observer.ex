defmodule Logflare.SystemMetrics.Observer do
  def get_metrics() do
    :observer_backend.sys_info()
    |> Keyword.drop([:alloc_info])
  end

  def get_memory() do
    :erlang.memory() |> Enum.map(fn {k, v} -> {k, div(v, 1024 * 1024)} end) |> Enum.into(%{})
  end

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
