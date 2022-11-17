defmodule LogflareWeb.EveryoneChannel do
  @moduledoc false
  use LogflareWeb, :channel

  def join("everyone", _payload, socket) do
    {:ok, socket}
  end
end
