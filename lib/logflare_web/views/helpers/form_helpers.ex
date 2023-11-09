defmodule LogflareWeb.Helpers.Forms do
  @moduledoc false
  use LogflareWeb, :html

  def section_header(text) do
    anchor = text |> String.downcase() |> String.replace(" ", "-")

    assigns = %{
      anchor: anchor,
      text: text
    }

    ~H"""
    <h5 id={@anchor} class="header-margin scroll-margin"><%= @text %> <%= link("#", to: "#" <> @anchor) %></h5>
    """
  end
end
