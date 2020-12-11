defmodule LogflareWeb.LiveHelpers do
  @moduledoc false
  import Phoenix.LiveView.Helpers

  def live_modal(socket, component, opts) do
    path = Keyword.fetch!(opts, :return_to)
    title = Keyword.fetch!(opts, :title)
    modal_opts = [id: :modal, return_to: path, component: component, opts: opts, title: title]
    live_component(socket, LogflareWeb.ModalComponent, modal_opts)
  end

  defmacro self_path(socket, extra \\ []) do
    Routes.live_path(socket, __MODULE__, Enum.into(extra, socket.assign.params))
  end

  defmacro self_path(socket, action, extra) do
    Routes.live_path(socket, __MODULE__, action, Enum.into(extra, socket.assign.params))
  end
end
