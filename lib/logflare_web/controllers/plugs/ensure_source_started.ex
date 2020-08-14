defmodule LogflareWeb.Plugs.EnsureSourceStarted do
  @moduledoc """
  Verifies that user is admin
  """
  use Plug.Builder

  require Logger

  alias Logflare.Source.Supervisor

  def call(%{assigns: %{source: %{token: source_id}}} = conn, _params) do
    case Process.whereis(source_id) do
      nil ->
        Logger.info("Source process not found, starting...", source_id: source_id)

        Supervisor.start_source(source_id)

        conn

      _else ->
        conn
    end
  end

  def call(conn, _params), do: conn
end
