defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller
  alias Logflare.Users

  require Logger

  alias Logflare.Sources
  alias Logflare.Google.BigQuery.EventUtils.Validator

  plug :check_user
  plug :check_source_token_and_name
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

  def check_source_token_and_name(conn, _opts) do
    source_token = conn.params["source"]
    source_name = conn.params["source_name"]

    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    user = Users.Cache.find_user_by_api_key(api_key)
    sources_strings = Enum.map(user.sources, &Atom.to_string(&1.token))

    cond do
      is_nil(source_token) and is_nil(source_name) ->
        message = "Source or source_name needed."

        conn
        |> put_status(403)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: message)
        |> halt()

      source_token in sources_strings ->
        source =
          source_token
          |> String.to_existing_atom()
          |> Sources.Cache.get_by_id()

        conn
        |> assign(:source, source)

      source_name ->
        source = Sources.Cache.get_by_name(source_name)

        if not is_nil(source) and Atom.to_string(source.token) in sources_strings do
          conn
          |> assign(:source, source)
        else
          message = "Source is not owned by this user."

          conn
          |> put_status(403)
          |> put_view(LogflareWeb.LogView)
          |> render("index.json", message: message)
          |> halt()
        end

      true ->
        message = "Source is not owned by this user."

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
