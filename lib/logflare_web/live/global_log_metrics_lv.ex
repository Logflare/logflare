defmodule LogflareWeb.GlobalLogMetricsLV do
  @moduledoc false
  alias Logflare.SystemMetrics.AllLogsLogged
  import Number.Delimit, only: [number_to_delimited: 1]
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, "live_widget.html"}

  def render(assigns) do
    ~L"""
    <h3>That's <span><%= @log_count %></span> events logged to date</h3>
    <h3>Counting <span><%= @per_second %></span> events per second</h3>
    """
  end

  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(250, self(), :tick)

    {:ok, put_data(socket)}
  end

  def handle_info(:tick, socket) do
    {:noreply, put_data(socket)}
  end

  defp put_data(socket) do
    import AllLogsLogged.Poller

    log_count =
      total_logs_logged_cluster()
      |> number_to_delimited

    total_logs_per_second =
      logs_last_second_cluster()
      |> number_to_delimited

    socket
    |> assign(:log_count, log_count)
    |> assign(:per_second, total_logs_per_second)
  end
end
