defmodule LogflareWeb.BillingPlansLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, "live_widget.html"}
  use Phoenix.HTML

  def mount(_params, _session, socket) do
    {:ok, assign(socket, :period, "year")}
  end

  def handle_event("switch_period", %{"period" => period}, socket) do
    {:noreply, assign(socket, :period, period)}
  end

  def render(assigns) do
    ~L"""
    <button phx-click="switch_period" phx-value-period=<%= period!(@period) %> class="btn btn-primary">See pricing per <%= @period %></button>
    """
  end

  defp period!("month"), do: :year
  defp period!("year"), do: :month
end
