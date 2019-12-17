defmodule Logflare.SystemMetrics.Observer do
  def get_metrics() do
    :observer_backend.sys_info()
    |> Keyword.drop([:alloc_info])
  end
end
