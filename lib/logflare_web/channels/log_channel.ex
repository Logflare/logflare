defmodule LogflareWeb.LogChannel do
  use LogflareWeb, :channel

  def join("ingest:" <> source_id, _payload, socket) do
    send(self, :after_join)
    {:ok, socket}
  end

  def handle_info(:after_join, socket) do
    push(socket, "notify", %{message: "Ready! Can we haz all your datas?"})
    {:noreply, socket}
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
    push(socket, "notify", %{
      message: "Unhandled message. Please verify.",
      payload: inspect(payload)
    })

    {:noreply, socket}
  end
end
