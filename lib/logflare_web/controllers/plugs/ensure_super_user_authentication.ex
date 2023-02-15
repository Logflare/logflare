defmodule LogflareWeb.Plugs.EnsureSuperUserAuthentication do
  @moduledoc false
  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts \\ []) do
    [token: token] = Application.get_env(:logflare, __MODULE__)

    case get_req_header(conn, "authorization") do
      ["Bearer " <> ^token] ->
        conn

      _ ->
        conn
        |> put_status(:unauthorized)
        |> Phoenix.Controller.json(%{"message" => "Error, invalid token!"})
        |> halt()
    end
  end
end
