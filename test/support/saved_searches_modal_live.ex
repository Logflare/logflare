defmodule LogflareWeb.SavedSearchesModalLive do
  @moduledoc false
  use LogflareWeb, {:live_view, layout: false}

  alias LogflareWeb.SearchLive.SavedSearchesModalComponent

  @impl true
  @spec mount(map(), map(), Phoenix.LiveView.Socket.t()) :: {:ok, Phoenix.LiveView.Socket.t()}
  def mount(_params, %{"user" => user, "params" => params}, socket) do
    {:ok, assign(socket, user: user, params: params)}
  end

  @impl true
  @spec render(map()) :: Phoenix.LiveView.Rendered.t()
  def render(assigns) do
    ~H"""
    <.live_component module={SavedSearchesModalComponent} id="saved-searches-modal" user={@user} params={@params} />
    """
  end

  @impl true
  @spec handle_info(tuple(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  def handle_info({:saved_searches_updated, _source_id, _searches}, socket) do
    {:noreply, socket}
  end

  def handle_info({:set_flash, {type, message}}, socket) do
    {:noreply, put_flash(socket, type, message)}
  end
end
