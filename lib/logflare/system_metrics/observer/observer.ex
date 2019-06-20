defmodule Logflare.SystemMetrics.Observer do
  def get_metrics() do
    :observer_backend.sys_info()
    |> Keyword.drop([:alloc_info])
    |> Enum.map(fn {x, y} ->
      if is_list(y) do
        {x, to_string(y)}
      else
        {x, y}
      end
    end)
    |> Enum.into(%{})
  end

  def get_memory() do
    :erlang.memory() |> Enum.map(fn {k, v} -> {k, div(v, 1024 * 1024)} end) |> Enum.into(%{})
  end

  def get_processes() do
    for {{:registered_name, name}, {:reductions, reds}} <-
          Stream.map(
            Process.list(),
            &{Process.info(&1, :registered_name), Process.info(&1, :reductions)}
          ),
        into: %{},
        do: {name, reds}
  end
end
