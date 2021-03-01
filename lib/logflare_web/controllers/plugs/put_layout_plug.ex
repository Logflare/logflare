defmodule LogflareWeb.Plugs.RootLayoutPlug do
  @moduledoc """
  This module exists only to prevent excessive recompilation of router.ex if it includes LayoutView module in it's code
  """
  def init(opts), do: opts

  def call(conn, _opts) do
    Phoenix.Controller.put_root_layout(conn, {LogflareWeb.LayoutView, :root})
  end
end
