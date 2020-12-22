defmodule LogflareWeb.ModalsLVHelpers do
  @moduledoc """
  Modal helpers to be imported where modals may be called
  """
  import LogflareWeb.SearchLV.Utils
  import Phoenix.LiveView, only: [assign: 3]

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
          if type in ["component", "live_view"] do
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
            "live_view" -> {:live_view, module_or_template}
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
end
