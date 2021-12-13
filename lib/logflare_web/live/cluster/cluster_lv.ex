defmodule LogflareWeb.ClusterLV do
  @moduledoc """
  Provides real-time data on cluster connectivity
  """
  use LogflareWeb, :live_view

  alias LogflareWeb.AdminClusterView
  alias Phoenix.LiveView.Socket

  require Logger

  def render(assigns) do
    AdminClusterView.render("index.html", assigns)
  end

  def mount(_params, _session, socket) do
    socket = assign_cluster_status(socket)
    :timer.send_interval(1_000, self(), :update_cluster_status)
    {:ok, socket}
  end

  def handle_event("shutdown", %{"node" => node}, socket) do
    msg = "Node shutdown initiated for #{node}"
    Logger.warn(msg)
    Logflare.Admin.shutdown(node)

    {:noreply, socket |> put_flash(:info, msg)}
  end

  def handle_event("shutdown", params, socket) do
    msg = "Node shutdown initiated for #{Node.self()}"
    Logger.warn(msg)
    Logflare.Admin.shutdown()

    {:noreply, socket |> put_flash(:info, msg)}
  end

  @spec handle_info(:update_cluster_status, Socket.t()) :: {:noreply, Socket.t()}
  def handle_info(:update_cluster_status, socket) do
    socket = assign_cluster_status(socket)
    {:noreply, socket}
  end

  def handle_info({:DOWN, _ref, :process, _from, :normal}, socket) do
    {:noreply, socket}
  end

  def handle_info({_from, :ok}, socket) do
    {:noreply, socket}
  end

  @spec assign_cluster_status(Socket.t()) :: Socket.t()
  def assign_cluster_status(socket) do
    assign(socket, nodes: Node.list(), node_self: Node.self())
  end
end
