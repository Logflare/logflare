defmodule LogflareWeb.LayoutView do
  use LogflareWeb, :view

  import LogflareWeb.Helpers.PageTitle

  @doc """
  Safely retrieves a value from the session, returning default if session hasn't been fetched.
  This prevents ArgumentError when rendering error pages before session fetch.
  """
  @spec safe_get_session(Plug.Conn.t(), atom(), any()) :: any()
  def safe_get_session(conn, key, default \\ nil) do
    if Map.get(conn.private, :plug_session_fetch) == :done do
      Plug.Conn.get_session(conn, key) || default
    else
      default
    end
  end
end
