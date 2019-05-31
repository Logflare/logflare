defmodule LogflareWeb.Plugs.VerifyApiRequest do
  use Plug.Builder
  import Phoenix.Controller
  alias Logflare.{Users, User, Source}

  require Logger

  alias Logflare.Sources

  plug :check_source_token_and_name
  plug :check_log_entry

  def check_log_entry(%{params: params} = conn, _opts \\ []) do

  def check_log_entry(%{params: params} = conn, _opts) do
    log_entry = params["log_entry"] || params["batch"]

    if log_entry in [%{}, [], "", nil] do
      message = "Log entry needed."

      conn
      |> put_status(403)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    else
      raw_logs =
        params
        |> Map.get("batch", params)
        |> List.wrap()

      conn
      |> assign(:log_events, raw_logs)
    end
  end

  def check_source_token_and_name(%{assigns: %{user: user}} = conn, _opts) do
    source_token = conn.params["source"]
    source_name = conn.params["source_name"]
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

        assign(conn, :source, source)

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
end
