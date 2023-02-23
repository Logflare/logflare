defmodule LogflareWeb.Plugs.PartnerAuthentication do
  @moduledoc false
  import Plug.Conn
  alias Logflare.Partners
  def init(opts), do: opts
  def call(conn, opts \\ [])

  def call(%{params: %{"token" => partner_token}} = conn, _opts) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> req_auth_token] -> check_token(partner_token, req_auth_token, conn)
      _ -> unauthorized(conn)
    end
  end

  def call(conn, _), do: unauthorized(conn)

  defp check_token(partner_token, req_auth_token, conn) do
    case Partners.get_partner_by_token(partner_token) do
      nil -> unauthorized(conn)
      partner -> compare_tokens(conn, partner, req_auth_token)
    end
  end

  defp compare_tokens(conn, %{auth_token: auth_token} = partner, req_auth_token) do
    case Plug.Crypto.secure_compare(auth_token, req_auth_token) do
      true -> assign(conn, :partner, partner)
      false -> unauthorized(conn)
    end
  end

  defp unauthorized(conn) do
    conn
    |> put_status(:unauthorized)
    |> Phoenix.Controller.json(%{"message" => "Invalid partner token"})
    |> halt()
  end
end
