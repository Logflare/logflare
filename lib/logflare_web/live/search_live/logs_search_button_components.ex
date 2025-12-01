defmodule LogflareWeb.SearchLive.LogsSearchButtonComponents do
  @moduledoc """
  Reusable button components for logs search controls.
  """
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  alias LogflareWeb.SearchLive.SearchComponents

  attr :tailing?, :boolean, required: true
  attr :play_event, :string, values: ["soft_play", "hard_play"]

  def live_pause_button(assigns) do
    ~H"""
    <span :if={@tailing?} class="btn btn-primary live-pause mr-0 text-nowrap" phx-click="soft_pause">
      <i class="spinner-border spinner-border-sm text-info" role="status"></i>
      <span class="fas-in-button hide-on-mobile" id="search-tailing-button">Pause</span>
    </span>
    <span :if={not @tailing?} class="btn btn-primary live-pause mr-0" phx-click={@play_event}>
      <i class="fas fa-play"></i><span class="fas-in-button hide-on-mobile">Live</span>
    </span>
    """
  end

  attr :tailing?, :boolean, required: true
  attr :uri_params, :map, required: true

  def navigation_buttons(assigns) do
    assigns =
      assigns
      |> assign(
        :play_event,
        if(assigns.uri_params["tailing"] == "true",
          do: "soft_play",
          else: "hard_play"
        )
      )

    ~H"""
    <div class="btn-group pr-2">
      <a href="#" phx-click="backwards" class="btn btn-primary mr-0">
        <span class="fas fa-step-backward"></span>
      </a>
      <.live_pause_button tailing?={@tailing?} play_event={@play_event} />
      <a href="#" phx-click="forwards" class="btn btn-primary">
        <span class="fas fa-step-forward"></span>
      </a>
    </div>
    """
  end

  attr :source, Logflare.Sources.Source, required: true
  attr :user, Logflare.User, required: true
  attr :has_results?, :boolean

  def action_buttons(assigns) do
    ~H"""
    <div class="pr-2 pt-1 pb-1">
      <a href="#" phx-click="save_search" class="btn btn-primary">
        <i class="fas fa-save"></i>
        <span class="fas-in-button hide-on-mobile">Save</span>
      </a>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <span class="btn btn-primary" id="daterangepicker">
        <i class="fas fa-clock"></i>
        <span class="hide-on-mobile fas-in-button">DateTime</span>
      </span>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <.link navigate={~p"/sources/#{@source}?querystring=c:count(*) c:group_by(t::minute)&tailing?=true"} class="btn btn-primary">
        <i class="fas fa-redo"></i>
        <span class="hide-on-mobile fas-in-button">Reset</span>
      </.link>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <SearchComponents.create_menu user={@user} disabled={@has_results? == false} />
    </div>
    """
  end
end
