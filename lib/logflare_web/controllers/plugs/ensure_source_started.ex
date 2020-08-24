defmodule LogflareWeb.Plugs.EnsureSourceStarted do
  @moduledoc """
  Verifies that user is admin
  """
  use Plug.Builder

  require Logger

  alias Logflare.Source.Supervisor

  def call(%{assigns: %{source: %{token: source_id}}} = conn, _params) do
    {:ok, _} = Supervisor.ensure_started(source_id)

    conn
  end

  def call(conn, _params), do: conn
end
