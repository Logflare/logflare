defmodule LogflareWeb.EveryoneChannel do
  use LogflareWeb, :channel

  def join("everyone", _payload, socket) do
    {:ok, socket}
  end
end
