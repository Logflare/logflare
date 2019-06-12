defmodule LogflareWeb.GlobalLogMetricsLV do
  @moduledoc false
  alias Logflare.SystemMetrics.AllLogsLogged
  import Number.Delimit, only: [number_to_delimited: 1]
  use Phoenix.LiveView

  def render(assigns) do
    ~L"""
      <span> <%= @log_count %> </span>
    """
  end

  def mount(_session, socket) do
    if connected?(socket), do: :timer.send_interval(1000, self(), :tick)

    {:ok, put_data(socket)}
  end

  def handle_info(:tick, socket) do
    {:noreply, put_data(socket)}
  end

  defp put_data(socket) do
    {:ok, log_count} = AllLogsLogged.log_count(:total_logs_logged)
    log_count = number_to_delimited(log_count)

    assign(socket, log_count: log_count)
  end
end
