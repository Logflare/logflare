defmodule LogflareWeb.Plugs.VerifyApiRequest do
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Source

  def init(_opts) do
  end

  def call(conn, _opts) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    source = conn.params["source"]
    source_name = conn.params["source_name"]
    log_entry = conn.params["log_entry"]

    conn
    |> check_api_key(api_key)
    |> check_log_entry(log_entry)
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

  defp check_source_token(conn, source) do
    cond do
      is_nil(source) ->
        conn

      String.length(source) == 36 ->
        case Repo.get_by(Source, token: source) do
          nil ->
            message = "Check your source."

            conn
            |> put_status(403)
            |> put_view(LogflareWeb.LogView)
            |> render("index.json", message: message)
            |> halt()

          _ ->
            conn
        end

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
