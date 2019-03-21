defmodule LogflareWeb.SourceChannel do
  use LogflareWeb, :channel

  def join("source:" <> source_token, _payload, socket) do
    if authorized?(source_token, socket) do
      {:ok, socket}
    else
      {:error, %{reason: "Not authorized!"}}
    end
  end

  defp authorized?(source_token, socket) do
    cond do
      socket.assigns[:user].admin ->
        true

      socket.assigns[:user] ->
        Enum.map(socket.assigns[:user].sources, & &1.token) |> Enum.member?(source_token)

      socket.assigns[:public_token] ->
        true

      true ->
        false
    end
  end
end
