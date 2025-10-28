defmodule LogflareWeb.ErrorView do
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

  def render("401.json", _assigns) do
    %{error: "Unauthorized"}
  end

  def render("403.json", _assigns) do
    %{error: "Forbidden"}
  end

  def render("404.json", _assigns) do
    %{error: "Not Found"}
  end

  def render("500.json", _assigns) do
    %{error: "Internal Server Error"}
  end

  def template_not_found(template, _assigns) do
    Phoenix.Controller.status_message_from_template(template)
  end
end
