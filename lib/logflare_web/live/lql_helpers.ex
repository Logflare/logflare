defmodule LogflareWeb.LqlHelpers do
  @moduledoc false
  use Phoenix.Component

  import LogflareWeb.ModalLiveHelpers

  alias LogflareWeb.SharedView

  def lql_help_modal_link(assigns) do
    ~H"""
    <.modal_link template="lql_help.html" view={SharedView} modal_id={:lql_help_link} title="Logflare Query Language">
      <i class="fas fa-code"></i><span class="hide-on-mobile"> LQL </span>
    </.modal_link>
    """
  end

  @doc """
  Renders a link to open the BigQuery source schema modal.
  """
  def bq_source_schema_modal_link(assigns) do
    ~H"""
    <.modal_link component={LogflareWeb.SourceBqSchemaComponent} modal_id={:bq_schema_link} title="Source Schema">
      <i class="fas fa-database"></i><span class="hide-on-mobile"> schema </span>
    </.modal_link>
    """
  end
end
