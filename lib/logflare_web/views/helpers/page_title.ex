defmodule LogflareWeb.Helpers.PageTitle do
  @moduledoc """
  Sets page titles.
  """
  alias LogflareWeb.{SourceView}

  # alias LogflareWeb.{SourceView, MarketingView}

  @suffix "Logflare | Cloudflare, Vercel & Elixir Logging"

  def page_title(assigns), do: assigns |> get |> put_suffix

  defp put_suffix(nil), do: @suffix
  defp put_suffix(title), do: title <> " | " <> @suffix

  # defp get(%{view_module: MarketingView}), do: "Works"

  defp get(%{view_module: SourceView, view_template: "show.html", source: source}) do
    source.name
  end

  defp get(_), do: nil
end
