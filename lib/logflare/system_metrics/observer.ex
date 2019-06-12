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
  end
end
