defmodule LogflareTelemetry.Reporters.BEAM.V0 do
  use GenServer
  require Logger
  @env Application.get_env(:logflare, :env)
  alias LogflareTelemetry.Reporters.Gen.V0, as: Reporter

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts[:metrics])
  end

  def init(metrics) do
    if @env != :test do
      Process.flag(:trap_exit, true)
    end

    attach_handlers(metrics)

    {:ok, %{}}
  end

  def attach_handlers(metrics) do
    metrics
    |> Enum.group_by(& &1.event_name)
    |> Enum.each(fn {event, metrics} ->
      id = {__MODULE__, event, self()}
      :telemetry.attach(id, event, &Reporter.handle_event/4, metrics)
    end)
  end

  def terminate(_, events) do
    Enum.each(events, &:telemetry.detach({__MODULE__, &1, self()}))
    :ok
  end
end
