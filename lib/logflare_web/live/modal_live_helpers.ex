defmodule LogflareWeb.ModalLiveHelpers do
  @moduledoc """
  Modal helpers to be imported where modals may be called
  """
  import Phoenix.Component, only: [assign: 3]
  import Phoenix.HTML.Link, only: [link: 2]

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

        module_or_template =
          case type do
            "template" -> module_or_template
            "component" -> module_or_template
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
              return_to: params["return-to"]
            },
            params: params
          })

        {:noreply, socket}
      end
    end
  end

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

    opts =
      [
        to: "#",
        phx_click: :show_live_modal,
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
      return_to: path,
      component: component,
      opts: opts,
      title: title,
      is_template?: false
    }

    Phoenix.Component.live_component(modal_opts)
  end
end
