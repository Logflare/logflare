defmodule LogflareWeb.ErrorView do
  @moduledoc """
  Error view for LogflareWeb html templates only
  All API errors are handled by the Api.FallbackController
  """
  use LogflareWeb, :view

  def render_in(name, assigns \\ %{}, do: inner) do
    render(name, Map.put(assigns, :inner_content, inner))
  end

  def render("401.html", assigns) do
    render("401_page.html", assigns)
  end

  def render("403.html", assigns) do
    render("403_page.html", assigns)
  end

  def render("404.html", assigns) do
    render("404_page.html", assigns)
  end

  def render("500.html", assigns) do
    render("500_page.html", assigns)
  end

  def template_not_found(template, _assigns) do
    Phoenix.Controller.status_message_from_template(template)
  end
end
