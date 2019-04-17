defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller

  require Logger

  alias Logflare.AccountCache
  alias Logflare.Google.BigQuery.EventUtils.Validator

  plug(:check_user)
  plug(:check_log_entry)
  plug(:check_source_and_name)
  plug(:check_source_token)
  plug(:validate_metadata)

  def check_user(conn, _opts) do
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

  def check_log_entry(conn, _opts) do
    log_entry = conn.params["log_entry"]

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

  def check_source_and_name(conn, _opts) do
    source = conn.params["source"]
    source_name = conn.params["source_name"]

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

  def check_source_token(conn, _opts) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]
    source = conn.params["source"]

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

  def validate_metadata(conn, _opts) do
    metadata = conn.params["metadata"]

    case Validator.valid?(metadata) do
      false ->
        message = "Check your metadata!"

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
