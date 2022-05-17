defmodule LogflareWeb.Plugs.VerifyApiAccess do
  @moduledoc """
  Verifies if a user has access to a requested resource.

  Assigns the token's associated user if the token is provided
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.Auth
  alias Logflare.{Users, User}

  def init(_), do: nil

  def call(%{request_path: "/endpoints/query" <> _} = conn, opts) do
    do_auth(:endpoints, conn, opts)
  end

  def call(%{request_path: _} = conn, opts) do
    do_auth(nil, conn, opts)
  end

  defp do_auth(:endpoints, conn, opts) do
    conn = fetch_query_params(conn)
    # fetch endpoint info
    with {:ok, token} <- extract_token(conn),
         {:ok, user} <- Auth.verify_access_token(token),
         endpoint_token <- conn.params["token"],
         endpoint <- Logflare.Endpoint.get_query_by_token(endpoint_token),
         true <- endpoint.user_id == user.id do
      assign(conn, :user, user)
    else
      _ ->
        send_error_response(conn, 401, "Error: Unauthorized")
    end
  end

  defp do_auth(_resource, conn, opts) do
    # unknown resource, reject as bad request
    send_error_response(conn, 400, "Error: Bad request")
  end

  defp extract_token(conn) do
    auth_header =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("authorization")

    bearer =
      if auth_header && String.contains?(auth_header, "Bearer ") do
        String.split(auth_header, " ")
        |> Enum.at(1)
      end

    api_key =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("x-api-key", conn.params["api_key"])

    cond do
      bearer != nil -> {:ok, bearer}
      api_key != nil -> {:ok, api_key}
      true -> {:error, :no_token}
    end
  end

  defp send_error_response(conn, code, message) do
    conn
    |> put_status(code)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: message)
    |> halt()
  end
end
