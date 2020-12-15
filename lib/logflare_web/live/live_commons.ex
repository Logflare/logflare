defmodule LogflareWeb.LiveCommons do
  import Phoenix.LiveView

  defmacro __using__(_context) do
    quote do
      def handle_info({:clear_flash, key}, socket) when is_atom(key) do
        {:noreply, clear_flash(socket, key)}
      end
    end
  end
end
