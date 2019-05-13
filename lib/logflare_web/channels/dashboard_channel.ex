defmodule LogflareWeb.DashboardChannel do
  use LogflareWeb, :channel

  def join("dashboard:" <> source_token, _payload, socket) do
    cond do
      is_admin?(socket) ->
        {:ok, socket}

      has_source?(source_token, socket) ->
        {:ok, socket}

      true ->
        {:error, %{reason: "Not authorized!"}}
    end
  end

  defp has_source?(source_token, socket) do
    Enum.map(socket.assigns[:user].sources, & &1.token) |> Enum.member?(String.to_existing_atom(source_token))
  end

  defp is_admin?(socket) do
    socket.assigns[:user].admin
  end
end
