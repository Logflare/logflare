defmodule LogflareWeb.LogChannel do
  use LogflareWeb, :channel

  def join("ingest", payload, socket) do
    push(socket, "ingest", %{message: "Socket ready"})
    {:ok, socket}
  end

  def handle_in("batch", payload, socket) do
    push(socket, "batch", %{message: "Handled batch"})
    {:noreply, socket}
  end

  def handle_in("ping", payload, socket) do
    push(socket, "pong", %{message: "Pong"})
    {:noreply, socket}
  end
end
