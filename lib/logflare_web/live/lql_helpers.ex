defmodule LogflareWeb.LqlHelpers do
  @moduledoc false
  use LogflareWeb, :view
  import LogflareWeb.ModalLiveHelpers
  alias LogflareWeb.SharedView

  def lql_help_modal_link() do
    live_modal_show_link(
      template: "lql_help.html",
      view: SharedView,
      modal_id: :lql_help_link,
      title: "Logflare Query Language"
    ) do
      assigns = %{}

      ~H"""
      <i class="fas fa-code"></i><span class="hide-on-mobile"> LQL </span>
      """
    end
  end

  def bq_source_schema_modal_link() do
    live_modal_show_link(
      component: LogflareWeb.SourceBqSchemaComponent,
      modal_id: :bq_schema_link,
      title: "Source Schema"
    ) do
      assigns = %{}

      ~H"""
      <i class="fas fa-database"></i><span class="hide-on-mobile"> schema </span>
      """
    end
  end
end
