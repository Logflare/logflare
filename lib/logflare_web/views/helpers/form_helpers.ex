defmodule LogflareWeb.Helpers.Forms do
  @moduledoc false
  use Phoenix.HTML

  def section_header(text) do
    anchor = String.downcase(text) |> String.replace(" ", "-")

    ~E"""
    <h5 id="<%= anchor %>" class="header-margin"><%= text %> <%= link "#", to: "#" <> anchor %></h5>
    """
  end
end
