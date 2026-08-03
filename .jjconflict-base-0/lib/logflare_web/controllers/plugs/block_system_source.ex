defmodule LogflareWeb.Plugs.BlockSystemSource do
  @moduledoc """
  Prevents ingestion and other forbidden actions on system monitoring sources
  """
  use Plug.Builder

  alias LogflareWeb.Api.FallbackController

  def call(%{assigns: %{source: %{system_source: true}}} = conn, _opts) do
    FallbackController.call(conn, {:error, :unauthorized})
  end

  def call(conn, _), do: conn
end
