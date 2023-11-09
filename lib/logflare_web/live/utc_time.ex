defmodule LogflareWeb.UtcTimeLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, :live_widget}

  def render(assigns) do
    ~H"""
    <span><%= @date %> UTC</span>
    """
  end

  def mount(_params, _session, socket) do
    if connected?(socket), do: :timer.send_interval(1000, self(), :tick)

    {:ok, put_date(socket)}
  end

  def handle_info(:tick, socket) do
    {:noreply, put_date(socket)}
  end

  def handle_event("nav", _path, socket) do
    {:noreply, socket}
  end

  defp put_date(socket) do
    date =
      Timex.now()
      |> Timex.to_datetime("Etc/UTC")
      |> Timex.format!("{h12}:{m}:{s}{am}")

    assign(socket, date: date)
  end
end
