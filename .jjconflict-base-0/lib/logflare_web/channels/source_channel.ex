defmodule LogflareWeb.SourceChannel do
  @moduledoc false
  use LogflareWeb, :channel

  alias Logflare.Sources.Cache, as: SourcesCache
  alias Logflare.Sources.Source

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
        case SourcesCache.get_by(token: source_token) do
          %Source{public_token: pt} when is_binary(pt) ->
            Plug.Crypto.secure_compare(pt, socket.assigns.public_token)

          _ ->
            false
        end

      socket.assigns[:user] && socket.assigns[:user].admin ->
        true

      socket.assigns[:user] ->
        Enum.map(socket.assigns[:user].sources, & &1.token)
        |> Enum.member?(String.to_existing_atom(source_token))

      true ->
        false
    end
  end
end
