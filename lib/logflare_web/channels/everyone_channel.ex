defmodule LogflareWeb.EveryoneChannel do
  use LogflareWeb, :channel

  def join("everyone", _payload, socket) do
    if authorized?() do
      {:ok, socket}
    else
      {:error, %{reason: "Not authorized!"}}
    end
  end

  defp authorized?() do
    true
  end
end
