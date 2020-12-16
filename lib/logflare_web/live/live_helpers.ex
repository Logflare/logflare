defmodule LogflareWeb.LiveHelpers do
  @moduledoc false
  import Phoenix.LiveView.Helpers

  def live_alert(socket, opts) do
    key = Keyword.fetch!(opts, :key)
    value = Keyword.fetch!(opts, :value)
    alert_class = Keyword.fetch!(opts, :alert_class)
    opts = [id: :"alert_#{key}", key: key, value: value, alert_class: alert_class]
    live_component(socket, LogflareWeb.AlertComponent, opts)
  end

  def live_modal(socket, component, opts) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)
    modal_opts = [id: :modal, return_to: path, component: component, opts: opts, title: title]
    live_component(socket, LogflareWeb.ModalComponent, modal_opts)
  end
end
