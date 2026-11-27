defmodule LogflareWeb.Plugs.DocsAssets do
  @moduledoc """
  Serves the images referenced by the in-app docs from `priv/docs`.

  Only image files inside the docs sections are served, never the markdown or
  metadata files next to them.
  """
  @behaviour Plug

  alias Plug.Conn

  @docs_dir Path.join(["docs", "docs.logflare.com", "docs"])
  @sections @docs_dir |> File.ls!() |> Enum.filter(&File.dir?(Path.join(@docs_dir, &1)))
  @extensions ~w(.png .gif)

  @impl Plug
  def init(_opts) do
    Plug.Static.init(
      at: "/docs/assets",
      from: {:logflare, "priv/docs"},
      gzip: false,
      only: @sections
    )
  end

  @impl Plug
  def call(%Conn{path_info: ["docs", "assets" | [_ | _] = path]} = conn, static_opts) do
    if image?(path), do: Plug.Static.call(conn, static_opts), else: conn
  end

  def call(conn, _static_opts), do: conn

  defp image?(path) do
    extension = path |> List.last() |> Path.extname() |> String.downcase()
    extension in @extensions
  end
end
