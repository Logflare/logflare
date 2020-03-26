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
      socket.assigns == %{} ->
        false

      socket.assigns[:public_token] ->
        true

      socket.assigns[:user][:admin] ->
        true

      socket.assigns[:user] ->
        Enum.map(socket.assigns[:user].sources, & &1.token)
        |> Enum.member?(String.to_existing_atom(source_token))

      true ->
        false
    end
  end
end
