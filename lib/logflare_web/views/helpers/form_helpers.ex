defmodule LogflareWeb.Helpers.Forms do
  @moduledoc false
  use Phoenix.HTML

  def section_header(text) do
    anchor = String.downcase(text) |> String.replace(" ", "-")

    ~E"""
    <a name="<%= anchor %>">
      <h5 class="header-margin"><%= text %> <%= link "#", to: "#" <> anchor %></h5>
    </a>
    """
  end
end
