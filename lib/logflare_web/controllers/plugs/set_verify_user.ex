defmodule LogflareWeb.Plugs.SetVerifyUser do
  @moduledoc """
  Assigns user if api key or browser session is present in conn
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Users, User}

  def init(_), do: nil

  def call(%{assigns: %{user: %User{}}} = conn, _opts), do: conn

  def call(%{request_path: "/api" <> _} = conn, opts), do: set_user_for_api(conn, opts)
  def call(%{request_path: "/logs" <> _} = conn, opts), do: set_user_for_api(conn, opts)

  def call(conn, opts), do: set_user_for_browser(conn, opts)

  def set_user_for_browser(conn, _opts) do
    user =
      conn
      |> get_session(:user_id)
      |> maybe_parse_binary_to_int()
      |> case do
        {int, ""} -> Users.Cache.get_by(id: int)
        _ -> nil
      end

    assign(conn, :user, user)
  end

  def set_user_for_api(conn, _opts) do
    api_key =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("x-api-key")

    case api_key && Users.Cache.get_by(api_key: api_key) do
      %User{} = user ->
        assign(conn, :user, user)

      _ ->
        message = "Error: please set API token"

        conn
        |> put_status(401)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
    end
  end

  defp maybe_parse_binary_to_int(int) when is_integer(int), do: int

  defp maybe_parse_binary_to_int(session_user_id) do
    session_user_id && Integer.parse(session_user_id)
  end
end
