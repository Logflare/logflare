defmodule LogflareWeb.LogChannel do
  use LogflareWeb, :channel

  def join("ingest:" <> source_id, _payload, socket) do
    push(socket, "notify", %{message: "Socket ready for channel `#{source_id}`"})
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

  def handle_in(event, payload, socket) do
    push(socket, event, %{message: event})
    {:noreply, socket}
  end
end
