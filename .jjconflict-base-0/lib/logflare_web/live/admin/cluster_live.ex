defmodule LogflareWeb.Admin.ClusterLive do
  @moduledoc false
  use LogflareWeb, :live_view

  alias Phoenix.LiveView.Socket
  alias Logflare.Admin

  require Logger

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :timer.send_interval(2_000, self(), :update_cluster_status)
    end

    {:ok, assign_cluster_status(socket)}
  end

  def handle_event("shutdown", %{"node" => node}, socket) do
    msg = "Node shutdown initiated for #{node}"
    Logger.warning(msg)

    String.to_existing_atom(node) |> Admin.shutdown()

    {:noreply, socket |> put_flash(:info, msg)}
  end

  @spec handle_info(:update_cluster_status, Socket.t()) :: {:noreply, Socket.t()}
  def handle_info(:update_cluster_status, socket) do
    {:noreply, assign_cluster_status(socket)}
  end

  def handle_info({:DOWN, _ref, :process, _from, :normal}, socket) do
    {:noreply, socket}
  end

  def handle_info({_ref, :ok}, socket) do
    {:noreply, socket}
  end

  @spec assign_cluster_status(Socket.t()) :: Socket.t()
  def assign_cluster_status(socket) do
    assign(socket, nodes: Node.list(), node_self: Node.self())
  end
end
