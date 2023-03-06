defmodule LogflareWeb.Plugs.VerifyApiAccess do
  @moduledoc """
  Verifies if a user has access to a requested resource.

  Assigns the token's associated user if the token is provided

  Authentication api key can either be through access tokens or legacy `user.api_key`.

  Access token usage is preferred and `user.api_key` is only used as a fallback.
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.Auth
  alias Logflare.Endpoints
  alias Logflare.Users

  def init(args), do: args |> Enum.into(%{})

  def call(conn, _opts) do
    conn.request_path
    |> case do
      "/api/endpoints/" <> _ -> :endpoints
      "/endpoints" <> _ -> :endpoints
      _ -> :generic
    end
    |> do_auth(conn)
  end

  defp do_auth(:endpoints, %{params: params} = conn) when is_map_key(params, "token") do
    conn = fetch_query_params(conn)
    # fetch endpoint info
    endpoint = Endpoints.get_query_by_token(conn.params["token"])
    enable_auth = Map.get(endpoint || %{}, :enable_auth)

    if conn.request_path =~ "/endpoints/query/" and enable_auth == false do
      conn
    else
      do_auth(:generic, conn)
    end
  end

  # no resource checking needed
  defp do_auth(_, conn) do
    # generic access
    with {:ok, user} <- identify_requestor(conn) do
      conn
      |> assign(:user, user)
    else
      _ ->
        send_error_response(conn, 401, "Error: Unauthorized")
    end
  end

  defp identify_requestor(conn) do
    extracted = extract_token(conn)

    with {:ok, access_token_or_api_key} <- extracted,
         {:ok, user} <- Auth.verify_access_token(access_token_or_api_key) do
      {:ok, user}
    else
      {:error, :no_token} = err ->
        err

      {:error, _} = err ->
        # try to use legacy api_key
        case Users.get_by(api_key: elem(extracted, 1)) do
          %_{} = user -> {:ok, user}
          _ -> err
        end
    end
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
