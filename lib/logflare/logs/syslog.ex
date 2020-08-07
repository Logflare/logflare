defmodule Logflare.Logs.Syslog do
  require Logger

  def handle_batch(batch) when is_list(batch) do
    Enum.map(batch, &%{"message" => &1})
  end
end
