defmodule LogflareWeb.Helpers.Forms do
  @moduledoc false
  use LogflareWeb, :html

  def section_header(text) do
    anchor = String.downcase(text) |> String.replace(" ", "-")
    assigns = %{}
    ~L"""
    <h5 id="<%= anchor %>" class="header-margin scroll-margin"><%= text %> <%= link "#", to: "#" <> anchor %></h5>
    """
  end
end
