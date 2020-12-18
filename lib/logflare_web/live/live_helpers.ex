defmodule LogflareWeb.LiveHelpers do
  @moduledoc false
  import Phoenix.LiveView.Helpers
  import Phoenix.HTML.Link, only: [link: 2]

  def live_alert(socket, opts) do
    key = Keyword.fetch!(opts, :key)
    value = Keyword.fetch!(opts, :value)
    alert_class = Keyword.fetch!(opts, :alert_class)
    opts = [id: :"alert_#{key}", key: key, value: value, alert_class: alert_class]
    live_component(socket, LogflareWeb.AlertComponent, opts)
  end

  def live_modal_show_link(content \\ [], opts)

  def live_modal_show_link(opts, do: block) when is_list(opts) do
    live_modal_show_link(block, opts)
  end

  def live_modal_show_link(contents, opts) when is_list(opts) do
    {type, module_or_template} =
      Enum.find(opts, &match?({k, _} when k in [:component, :live_view, :template], &1))

    if type == :component or type == :live_view do
      IO.inspect(Module.concat(:"Elixir", module_or_template).__info__(:attributes))
      IO.inspect(Module.concat(:"Elixir", module_or_template).__info__(:macros))
    end

    id = Keyword.fetch!(opts, :id)
    title = Keyword.fetch!(opts, :title)
    view = Keyword.get(opts, :view)

    opts =
      [
        to: "#",
        phx_click: :show_live_modal,
        phx_value_module_or_template: module_or_template,
        phx_value_type: type,
        phx_value_id: id,
        phx_value_title: title,
        phx_value_view: view
      ] ++ opts

    link(contents, opts)
  end

  def live_modal(socket, {:live_view, lv}, opts) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)
    session = Keyword.fetch!(opts, :session)

    modal_opts = [
      id: :modal,
      return_to: path,
      opts: opts,
      title: title,
      lv: lv,
      session: session,
      live_view: lv
    ]

    live_component(socket, LogflareWeb.ModalComponent, modal_opts)
  end

  def live_modal(socket, template, opts)
      when is_binary(template) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)

    modal_opts = [
      id: :modal,
      return_to: path,
      template: template,
      opts: opts,
      title: title,
      view: Keyword.fetch!(opts, :view),
      is_template?: true
    ]

    live_component(socket, LogflareWeb.ModalComponent, modal_opts)
  end

  def live_modal(socket, component, opts) when is_atom(component) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)

    modal_opts = [
      id: :modal,
      return_to: path,
      component: component,
      opts: opts,
      title: title,
      is_template?: false
    ]

    live_component(socket, LogflareWeb.ModalComponent, modal_opts)
  end
end
