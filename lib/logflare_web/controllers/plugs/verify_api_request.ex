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
      |> check_source_token(source)
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

  defp check_source_token(conn, source) do
    cond do
      is_nil(source) ->
        conn
      true ->
        cond do
          String.length(source) == 36 ->
            conn
          true ->
            message = "Check your source."
            conn
            |> put_status(403)
            |> put_view(LogflareWeb.LogView)
            |> render("index.json", message: message)
            |> halt()
        end
    end
  end

end
