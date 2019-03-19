defmodule LogflareWeb.Plugs.VerifyApiRequest do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.AccountCache

  def init(_opts) do
  end

  def call(conn, _opts) do
    source = conn.params["source"]
    source_name = conn.params["source_name"]
    log_entry = conn.params["log_entry"]
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    conn
    |> check_user()
    |> check_log_entry(log_entry)
    |> check_source_and_name(source, source_name)
    |> check_source_token(source, api_key)
  end

  defp check_user(conn) do
    case conn.assigns.user do
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

  defp check_log_entry(conn, log_entry) do
    case log_entry == nil do
      true ->
        message = "Log entry needed."

        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()

      false ->
        conn
    end
  end

  defp check_source_and_name(conn, source, source_name) do
    case [source, source_name] do
      [nil, nil] ->
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

  defp check_source_token(conn, source, api_key) do
    cond do
      is_nil(source) ->
        conn

      is_nil(AccountCache.get_source(api_key, source)) ->
        message = "Check your source."

        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()

      true ->
        conn
    end
  end
end
