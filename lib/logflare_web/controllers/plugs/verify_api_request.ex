defmodule LogflareWeb.Plugs.VerifyApiRequest do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Repo
  alias Logflare.User

  def init(_params) do

  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    source = conn.params["source"]
    source_name = conn.params["source_name"]

    conn
      |> check_api_key(api_key)
      |> check_source_and_name(source, source_name)
  end

  defp check_api_key(conn, api_key) do
    case Repo.get_by(User, api_key: api_key) do
      nil ->
        message = "Unknown x-api-key."
        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
      _ ->
        conn
    end
  end

  defp check_source_and_name(conn, source, source_name) do
    case [source, source_name] do
      [nil, nil]  ->
        message = "Source or source_name needed."
        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()
      _ ->
        conn
    end
  end

end
