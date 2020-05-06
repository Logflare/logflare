defmodule LogflareWeb.ErrorView do
  use LogflareWeb, :view

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
