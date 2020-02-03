defmodule LogflareWeb.Plugs.CheckSourceCountApi do
  @moduledoc false
  import Phoenix.Controller
  import Plug.Conn

  def init(_opts) do
  end

  def call(%{user: %{sources: sources}} = conn, _opts) when length(sources) <= 100 do
    conn
  end

  def call(conn, _opts) do
    message = "You have 100 sources. Delete one first!"

    conn
    |> put_status(403)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: message)
    |> halt()
  end
end
