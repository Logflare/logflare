defmodule Logflare.TestLoggerCaptureHandler do
  @moduledoc false

  @spec log(map(), map()) :: :ok
  def log(event, %{config: %{pid: pid}}) do
    send(pid, {:log_event, event})
    :ok
  end
end
