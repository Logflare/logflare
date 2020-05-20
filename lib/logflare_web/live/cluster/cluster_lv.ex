defmodule LogflareWeb.ClusterLV do
  @moduledoc """
  Provides real-time data on cluster connectivity
  """
  alias LogflareWeb.AdminClusterView
  use Phoenix.LiveView, layout: {LogflareWeb.LayoutView, "live.html"}
  alias Phoenix.LiveView.Socket

  def render(assigns) do
    AdminClusterView.render("index.html", assigns)
  end

  def mount(_params, _session, socket) do
    socket = assign_cluster_status(socket)
    :timer.send_interval(1_000, self(), :update_cluster_status)
    {:ok, socket}
  end

  @spec handle_info(:update_cluster_status, Socket.t()) :: {:noreply, Socket.t()}
  def handle_info(:update_cluster_status, socket) do
    socket = assign_cluster_status(socket)
    {:noreply, socket}
  end

  @spec assign_cluster_status(Socket.t()) :: Socket.t()
  def assign_cluster_status(socket) do
    assign(socket, nodes: Node.list(), node_self: Node.self())
  end
end
