defmodule LogflareWeb.DashboardChannel do
  use LogflareWeb, :channel

  def join("dashboard:" <> source_token, _payload, socket) do
    if authorized?(source_token, socket) do
      {:ok, socket}
    else
      {:error, %{reason: "Not authorized!"}}
    end
  end

  defp authorized?(source_token, socket) do
    Enum.map(socket.assigns[:user].sources, & &1.token) |> Enum.member?(source_token)
  end
end
