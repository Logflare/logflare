defmodule LogflareWeb.SearchLive.SavedSearchesModalComponent do
  @moduledoc """
  Renders the saved searches modal content for logs search.
  """
  use LogflareWeb, :live_component

  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias Phoenix.LiveView.JS

  @spec update(map(), Phoenix.LiveView.Socket.t()) :: {:ok, Phoenix.LiveView.Socket.t()}
  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, assign_saved_searches(socket)}
  end

  @spec handle_event(String.t(), map(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  def handle_event("delete_saved_search", %{"id" => saved_search_id}, socket) do
    user = socket.assigns.user

    socket =
      with %Logflare.SavedSearch{} = saved_search <- SavedSearches.get(saved_search_id),
           true <- Sources.get_by_user_access(user, saved_search.source_id) |> is_struct(),
           {:ok, _response} <- SavedSearches.delete_by_user(saved_search) do
        _ = SavedSearches.Cache.bust_by(source_id: saved_search.source_id)
        send(self(), {:set_flash, {:info, "Saved search deleted"}})

        update(%{saved_searches: nil}, socket)
        |> elem(1)
      else
        nil ->
          send(self(), {:set_flash, {:error, "Saved search not found"}})

        false ->
          send(
            self(),
            {:set_flash, {:error, "You don't have permission to delete this saved search"}}
          )

        _ ->
          send(self(), {:set_flash, {:error, "Something went wrong!"}})
      end

    {:noreply, socket}
  end

  @spec render(map()) :: Phoenix.LiveView.Rendered.t()
  def render(assigns) do
    assigns =
      assigns
      |> assign_new(:team, fn -> nil end)

    ~H"""
    <div id="saved-searches-modal">
      <.async_result :let={saved_searches} assign={@saved_searches}>
        <:loading>{live_react_component("Components.Loader", %{}, id: "shared-loader")}</:loading>
        <:failed :let={_failure}>
          <div class="tw-flex tw-items-center tw-justify-center tw-min-h-[320px] tw-w-full tw-text-center tw-text-gray-500 tw-text-sm">
            Failed to load saved searches
          </div>
        </:failed>
        <.saved_searches_empty :if={Enum.empty?(saved_searches)} />
        <.saved_searches_list :if={Enum.any?(saved_searches)} saved_searches={saved_searches} source={@source} team={@team} myself={@myself} />
      </.async_result>
    </div>
    """
  end

  attr :saved_searches, :list, required: true
  attr :source, Logflare.Sources.Source, required: true
  attr :team, :any, default: nil
  attr :myself, :any, required: true

  defp saved_searches_list(assigns) do
    ~H"""
    <ul class="list-unstyled tw-mb-0" id="saved-searches-list">
      <li :for={saved_search <- @saved_searches} id={"saved-search-#{saved_search.id}"} class="tw-flex tw-items-center tw-justify-between tw-py-2">
        <.link
          patch={
            LogflareWeb.Utils.with_team_param(
              ~p"/sources/#{@source}/search?#{%{querystring: saved_search.querystring, tailing?: saved_search.tailing}}",
              @team
            )
          }
          class="tw-text-white tw-text-sm"
        >
          {saved_search.querystring}
        </.link>
        <button
          type="button"
          phx-click={JS.add_class("tw-hidden", to: "#saved-search-#{saved_search.id}") |> JS.push("delete_saved_search", target: @myself)}
          phx-value-id={saved_search.id}
          phx-target={@myself}
          phx-confirm="Delete saved search?"
          class="tw-text-xs tw-ml-2 tw-text-white tw-bg-transparent tw-border-none"
        >
          <i class="fa fa-trash"></i>
        </button>
      </li>
    </ul>
    """
  end

  defp saved_searches_empty(assigns) do
    ~H"""
    <div id="saved-searches-empty" class="tw-flex tw-items-center tw-justify-center tw-min-h-[320px] tw-w-full tw-text-center tw-text-gray-500 tw-text-sm">
      no saved searches
    </div>
    """
  end

  @spec assign_saved_searches(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  defp assign_saved_searches(socket) do
    source_id = socket.assigns.source.id

    socket
    |> assign_async(
      :saved_searches,
      fn ->
        saved_searches =
          SavedSearches.Cache.list_saved_searches_by_source(source_id)

        {:ok, %{saved_searches: saved_searches}}
      end
    )
  end
end
