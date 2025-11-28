defmodule LogflareWeb.ModalLiveHelpers do
  @moduledoc """
  Modal helpers to be imported where modals may be called
  """
  use Phoenix.Component

  import Phoenix.HTML.Link, only: [link: 2]

  alias Phoenix.LiveView.JS

  defmacro __using__(_context) do
    quote do
      def handle_info(:hide_modal, socket) do
        {:noreply, assign(socket, :show_modal, false)}
      end

      def handle_event(
            "show_live_modal",
            %{
              "module-or-template" => module_or_template,
              "type" => type,
              "id" => id,
              "title" => title
            } = params,
            socket
          ) do
        module_or_template =
          if type == "component" do
            String.to_existing_atom(module_or_template)
          else
            module_or_template
          end

        id = String.to_existing_atom(id)

        view =
          if v = params["view"] do
            String.to_existing_atom(v)
          end

        socket =
          socket
          |> assign(:show_modal, true)
          |> assign(:modal, %{
            body: %{
              module_or_template: module_or_template,
              view: view,
              title: title,
              id: id,
              close: params["close"],
              return_to: params["return-to"]
            },
            params: params
          })

        {:noreply, socket}
      end
    end
  end

  @doc """
  Creates a link to show a modal when clicked.

  # Customize click behaviour
  You can optionally assign a custom JS command to `click` which will be executed before showing the modal.

  ```
    <%= live_modal_show_link(component: LogflareWeb.MyComponent, click: JS.push("some_event"), ...) %>
  ```
  """
  def live_modal_show_link(content \\ [], opts)

  def live_modal_show_link(opts, do: block) when is_list(opts) do
    live_modal_show_link(block, opts)
  end

  def live_modal_show_link(contents, opts) when is_list(opts) do
    {type, module_or_template} =
      Enum.find(opts, &match?({k, _} when k in [:component, :live_view, :template], &1))

    id = Keyword.fetch!(opts, :modal_id)
    title = Keyword.fetch!(opts, :title)
    view = Keyword.get(opts, :view)
    return_to = Keyword.get(opts, :return_to)

    click =
      case Keyword.get(opts, :click) do
        nil ->
          :show_live_modal

        js_command when is_struct(js_command, JS) ->
          js_command
          |> JS.push("show_live_modal")
      end

    opts =
      [
        to: "#",
        phx_click: click,
        phx_value_close: Keyword.get(opts, :close),
        phx_value_module_or_template: module_or_template,
        phx_value_type: type,
        phx_value_id: id,
        phx_value_title: title,
        phx_value_return_to: return_to,
        phx_value_view: view
      ] ++ opts

    link(contents, opts)
  end

  def live_modal(template, opts)
      when is_binary(template) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)

    modal_opts = %{
      module: LogflareWeb.ModalComponent,
      id: :modal,
      return_to: path,
      template: template,
      opts: opts,
      title: title,
      view: Keyword.fetch!(opts, :view),
      is_template?: true
    }

    Phoenix.Component.live_component(modal_opts)
  end

  def live_modal(component, opts) when is_atom(component) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)

    modal_opts = %{
      module: LogflareWeb.ModalComponent,
      id: :"logflare-modal",
      close: Keyword.get(opts, :close),
      return_to: path,
      component: component,
      opts: opts,
      title: title,
      is_template?: false
    }

    Phoenix.Component.live_component(modal_opts)
  end

  @doc """
  Component version of live_modal_show_link for use in HEEx templates.

  ## Example

      <.modal_link component={LogflareWeb.MyComponent} modal_id={:my_modal} title="My Modal">
        <span>Open Modal</span>
      </.modal_link>
  """
  attr :component, :atom, default: nil
  attr :live_view, :atom, default: nil
  attr :template, :string, default: nil
  attr :modal_id, :atom, required: true
  attr :title, :string, required: true
  attr :view, :atom, default: nil
  attr :return_to, :string, default: nil
  attr :click, JS, default: nil
  attr :close, JS, default: nil
  attr :class, :string, default: nil
  attr :rest, :global
  slot :inner_block, required: true

  def modal_link(assigns) do
    {type, module_or_template} =
      cond do
        assigns.component -> {:component, assigns.component}
        assigns.live_view -> {:live_view, assigns.live_view}
        assigns.template -> {:template, assigns.template}
        true -> raise "Must provide one of :component, :live_view, or :template"
      end

    click =
      case assigns.click do
        nil -> JS.push("show_live_modal")
        js_command when is_struct(js_command, JS) -> JS.push(js_command, "show_live_modal")
      end

    assigns =
      assigns
      |> Phoenix.Component.assign(:type, type)
      |> Phoenix.Component.assign(:module_or_template, module_or_template)
      |> Phoenix.Component.assign(:phx_click, click)

    ~H"""
    <.link href="#" class={@class} phx-click={@phx_click} phx-value-close={@close} phx-value-module-or-template={@module_or_template} phx-value-type={@type} phx-value-id={@modal_id} phx-value-title={@title} phx-value-return-to={@return_to} phx-value-view={@view} {@rest}>
      {render_slot(@inner_block)}
    </.link>
    """
  end
end
