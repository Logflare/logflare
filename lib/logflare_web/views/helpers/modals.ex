defmodule LogflareWeb.Helpers.Modals do
  @moduledoc false
  use Phoenix.HTML

  def modal_link(modal_id, icon_classes, text) do
    ~E"""
    <a class="modal-link" href="#" phx-click="activate_modal" phx-value-modal_id="<%= modal_id %>"><span><i class="<%= icon_classes %>"></i></span> <span class="hide-on-mobile"><%= text %></span></a>
    """
  end
end
