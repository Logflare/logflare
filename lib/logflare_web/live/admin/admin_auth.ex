defmodule LogflareWeb.AdminLive.AdminAuth do
  @moduledoc false

  import Phoenix.LiveView

  alias Logflare.Admin

  @spec on_mount(:ensure_admin, map(), map(), Phoenix.LiveView.Socket.t()) ::
          {:cont, Phoenix.LiveView.Socket.t()} | {:halt, Phoenix.LiveView.Socket.t()}
  def on_mount(:ensure_admin, _params, %{"current_email" => email}, socket) do
    if Admin.admin?(email) do
      {:cont, socket}
    else
      {:halt, redirect(socket, to: "/")}
    end
  end

  def on_mount(:ensure_admin, _params, _session, socket) do
    {:halt, redirect(socket, to: "/")}
  end
end
