defmodule LogflareWeb.GlobalLogMetricsLV do
  @moduledoc false
  alias Logflare.SystemMetrics.AllLogsLogged
  import Number.Delimit, only: [number_to_delimited: 1]
  use Phoenix.LiveView

  def render(assigns) do
    ~L"""
    <h3>That's <span><%= @log_count %></span> events logged to date</h3>
    <h3>Counting <span><%= @per_second %></span> events per second</h3>
    """
  end

  def mount(_session, socket) do
    if connected?(socket), do: :timer.send_interval(100, self(), :tick)

    {:ok, put_data(socket)}
  end

  def handle_info(:tick, socket) do
    {:noreply, put_data(socket)}
  end

  defp put_data(socket) do
    {:ok, log_count} = AllLogsLogged.log_count(:total_logs_logged)
    total_logs_per_second = AllLogsLogged.Poller.get_total_logs_per_second()
    log_count = number_to_delimited(log_count)
    total_logs_per_second = number_to_delimited(total_logs_per_second)

    assign(socket, log_count: log_count, per_second: total_logs_per_second)
  end
end
