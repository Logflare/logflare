defmodule LogflareWeb.SearchLive.SavedSearchesModalComponent do
  @moduledoc """
  Renders the saved searches modal content for logs search.

  Set `edit-search-id` to the ID of a saved search. The modal shows that search
  first and opens a form to edit its name.
  """
  use LogflareWeb, :live_component

  alias Logflare.SavedSearch
  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias LogflareWeb.ItemListComponents
  alias Phoenix.LiveView.AsyncResult
  alias Phoenix.LiveView.JS

  @spec update(map(), Phoenix.LiveView.Socket.t()) :: {:ok, Phoenix.LiveView.Socket.t()}
  def update(assigns, socket) do
    source_id = assigns.params["source-id"] |> String.to_integer()
    user = assigns[:team_user] || assigns.user
    initial_edit_id = assigns.params["edit-search-id"]
    load_scope = {source_id, user, initial_edit_id}

    socket =
      socket
      |> assign(source_id: source_id)
      |> assign(user: user)
      |> assign(team: assigns[:team])

    socket =
      if socket.assigns[:load_scope] == load_scope do
        socket
      else
        socket
        |> assign(load_scope: load_scope, name_form: nil, initial_edit_id: initial_edit_id)
        |> assign_saved_searches()
      end

    {:ok, socket}
  end

  @spec handle_event(String.t(), map(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  def handle_event("edit_name", %{"id" => saved_search_id}, socket) do
    {:noreply,
     assign(socket,
       name_form: name_form(socket.assigns.saved_searches.result || [], saved_search_id),
       initial_edit_id: nil
     )}
  end

  def handle_event(
        "update_saved_search",
        %{"id" => saved_search_id, "saved_search" => params},
        socket
      ) do
    %{user: user, source_id: source_id} = socket.assigns

    socket =
      with %SavedSearch{source_id: ^source_id} = saved_search <-
             SavedSearches.get_by_user_access(user, saved_search_id),
           {:ok, updated} <- SavedSearches.update(saved_search, params) do
        saved_searches = SavedSearches.list_by_sources([source_id])
        send(self(), {:saved_searches_updated, source_id, saved_searches})

        message =
          if updated.saved_by_user, do: "Saved search updated", else: "Saved search deleted"

        send(self(), {:set_flash, {:info, message}})

        socket
        |> assign(:saved_searches, AsyncResult.ok(saved_searches))
        |> assign(:name_form, nil)
        |> assign(:initial_edit_id, nil)
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          assign(socket, :name_form, to_form(changeset))

        nil ->
          send(self(), {:set_flash, {:error, "Saved search not found"}})
          socket

        %SavedSearch{} ->
          send(
            self(),
            {:set_flash, {:error, "You don't have permission to update this saved search"}}
          )

          socket
      end

    {:noreply, socket}
  end

  def handle_event("cancel_rename", _params, socket) do
    {:noreply, assign(socket, name_form: nil, initial_edit_id: nil)}
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
        <.saved_searches_list saved_searches={saved_searches} source_id={@source_id} team={@team} myself={@myself} name_form={@name_form} initial_edit_id={@initial_edit_id} />
      </.async_result>
    </div>
    """
  end

  attr :saved_searches, :list, required: true
  attr :source_id, :any, required: true
  attr :team, :any, default: nil
  attr :myself, :any, required: true
  attr :name_form, :any, required: true
  attr :initial_edit_id, :any, required: true

  defp saved_searches_list(assigns) do
    assigns =
      assign(assigns,
        saved_searches:
          Enum.sort_by(assigns.saved_searches, &(to_string(&1.id) != assigns.initial_edit_id)),
        name_form: assigns.name_form || name_form(assigns.saved_searches, assigns.initial_edit_id)
      )

    ~H"""
    <ItemListComponents.item_list id="saved-searches-list" items={@saved_searches} class={["tw-max-h-[70vh] tw-overflow-y-auto", @saved_searches != [] && "tw-pr-2"]} row_id={&"saved-search-#{&1.id}"}>
      <:empty>
        <div id="saved-searches-empty" class="tw-flex tw-items-center tw-justify-center tw-min-h-[320px] tw-w-full tw-text-center tw-text-gray-500 tw-text-sm">
          no saved searches
        </div>
      </:empty>
      <:item :let={saved_search}>
        <div class="tw-flex tw-items-start tw-min-w-0 tw-flex-1">
          <button
            id={"favorite-saved-search-#{saved_search.id}"}
            type="button"
            phx-click={JS.push("update_saved_search", value: %{saved_search: %{favorite: !saved_search.favorite}})}
            phx-value-id={saved_search.id}
            phx-target={@myself}
            aria-pressed={to_string(saved_search.favorite)}
            aria-label={if saved_search.favorite, do: "Unstar saved search", else: "Star saved search"}
            class="tw-text-sm tw-text-yellow-200 tw-bg-transparent tw-border-none tw-p-0 tw-mr-3 tw-shrink-0"
          >
            <i class={[if(saved_search.favorite, do: "fas", else: "far"), "fa-star"]} aria-hidden="true"></i>
          </button>
          <.saved_search_name saved_search={saved_search} name_form={@name_form} source_id={@source_id} team={@team} myself={@myself} />
        </div>
      </:item>
    </ItemListComponents.item_list>
    """
  end

  attr :saved_search, :map, required: true
  attr :name_form, :any, required: true
  attr :source_id, :any, required: true
  attr :team, :any, default: nil
  attr :myself, :any, required: true

  @spec saved_search_name(map()) :: Phoenix.LiveView.Rendered.t()
  defp saved_search_name(%{name_form: %{data: %{id: id}}, saved_search: %{id: id}} = assigns) do
    ~H"""
    <div class="tw-flex tw-flex-col tw-min-w-0 tw-flex-1">
      <.form for={@name_form} id={"rename-saved-search-#{@saved_search.id}"} phx-submit={JS.push("update_saved_search", value: %{id: @saved_search.id})} phx-target={@myself}>
        <div class="tw-flex tw-items-center tw-gap-2">
          {text_input(@name_form, :name,
            id: "saved-search-name-#{@saved_search.id}",
            class: "form-control form-control-sm tw-min-w-0 tw-text-sm tw-h-6 tw-py-0",
            maxlength: 255,
            "aria-label": "Saved search name",
            "phx-mounted": JS.focus(),
            "phx-keydown": "cancel_rename",
            "phx-key": "escape",
            "phx-target": @myself
          )}
          <button type="submit" id={"save-saved-search-name-#{@saved_search.id}"} aria-label="Save name" class="tw-bg-transparent tw-border-none tw-p-0 tw-text-white tw-shrink-0" phx-disable-with="…">
            <i class="fa fa-check" aria-hidden="true"></i>
          </button>
          <button type="button" id={"cancel-saved-search-name-#{@saved_search.id}"} aria-label="Cancel renaming" phx-click="cancel_rename" phx-target={@myself} class="tw-bg-transparent tw-border-none tw-p-0 tw-text-white tw-shrink-0">
            <i class="fa fa-times" aria-hidden="true"></i>
          </button>
        </div>
        {error_tag(@name_form, :name)}
      </.form>
      <span class="tw-text-sm tw-text-gray-400 tw-truncate">{@saved_search.querystring}</span>
    </div>
    """
  end

  defp saved_search_name(assigns) do
    ~H"""
    <div class="tw-flex tw-flex-col tw-min-w-0 tw-flex-1">
      <div class="tw-group/saved-search tw-flex tw-items-center tw-min-w-0">
        <.link
          patch={
            LogflareWeb.Utils.with_team_param(
              ~p"/sources/#{@source_id}/search?#{%{querystring: @saved_search.querystring, tailing?: @saved_search.tailing}}",
              @team
            )
          }
          class="tw-text-white tw-text-sm tw-truncate"
        >
          {display_name(@saved_search)}
        </.link>
        <button
          type="button"
          id={"edit-saved-search-name-#{@saved_search.id}"}
          aria-label="Rename saved search"
          phx-click="edit_name"
          phx-value-id={@saved_search.id}
          phx-target={@myself}
          class="[@media(hover:hover)_and_(pointer:fine)]:tw-opacity-0 group-hover/saved-search:tw-opacity-100 group-focus-within/saved-search:tw-opacity-100 tw-bg-transparent tw-border-none tw-p-0 tw-ml-2 tw-text-gray-400 tw-text-xs tw-shrink-0"
        >
          <i class="fa fa-pencil-alt" aria-hidden="true"></i>
        </button>
      </div>
      <span class="tw-text-sm tw-text-gray-400 tw-truncate">{@saved_search.querystring}</span>
    </div>
    <button
      id={"delete-saved-search-#{@saved_search.id}"}
      type="button"
      phx-click={JS.add_class("tw-hidden", to: "#saved-search-#{@saved_search.id}") |> JS.push("update_saved_search", target: @myself, value: %{saved_search: %{saved_by_user: false}})}
      phx-value-id={@saved_search.id}
      phx-target={@myself}
      phx-confirm="Delete saved search?"
      class="tw-self-start tw-text-sm tw-leading-5 tw-p-0 tw-ml-2 tw-text-white tw-bg-transparent tw-border-none"
    >
      <i class="fa fa-trash"></i>
    </button>
    """
  end

  @spec display_name(SavedSearch.t()) :: String.t()
  def display_name(%SavedSearch{name: name}), do: name || "untitled"

  @spec name_form([SavedSearch.t()], String.t() | nil) :: Phoenix.HTML.Form.t() | nil
  defp name_form(saved_searches, id) do
    case Enum.find(saved_searches, &(to_string(&1.id) == id)) do
      nil -> nil
      saved_search -> to_form(SavedSearch.changeset(saved_search))
    end
  end

  @spec assign_saved_searches(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  defp assign_saved_searches(socket) do
    %{user: user, source_id: source_id, initial_edit_id: initial_edit_id} = socket.assigns
    context = if initial_edit_id, do: SavedSearches, else: SavedSearches.Cache

    assign_async(
      socket,
      :saved_searches,
      fn ->
        if Sources.get_by_user_access(user, source_id) do
          saved_searches = context.list_by_sources([source_id])
          {:ok, %{saved_searches: saved_searches}}
        else
          {:error, :not_found}
        end
      end
    )
  end
end
