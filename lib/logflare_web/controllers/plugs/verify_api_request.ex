defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller
  alias Logflare.Users

  require Logger

  alias Logflare.AccountCache
  alias Logflare.Google.BigQuery.EventUtils.Validator

  plug :check_user
  plug :check_source_and_name
  plug :check_source_token
  plug :check_log_entry
  plug :validate_metadata

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
    log_entry = conn.params["log_entry"] || conn.params["batch"]

    if is_nil(log_entry) || log_entry in [%{}, []] do
      message = "Log entry needed."

      conn
      |> put_status(403)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    else
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
    source_id = conn.params["source"] |> String.to_atom()

    user = Users.Cache.find_user_by_api_key(api_key)

    if Users.Cache.source_id_owned?(user, source_id) do
      conn
      |> assign(:source_id, source_id)
    else
      message = "Check your source."

      conn
      |> put_status(403)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    end
  end

  def validate_metadata(conn, _opts) do
    metadata = conn.params["metadata"]

    case is_nil(metadata) do
      true ->
        conn

      false ->
        case Validator.valid?(metadata) do
          false ->
            message = "Metadata keys failed validation. Check your metadata!"

            conn
            |> put_status(400)
            |> put_view(LogflareWeb.LogView)
            |> render("index.json", message: message)
            |> halt()

          true ->
            conn
        end
    end
  end
end
