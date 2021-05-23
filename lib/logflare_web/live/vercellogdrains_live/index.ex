defmodule LogflareWeb.VercelLogDrainsLive do
  @moduledoc """
  Vercel Log Drain edit LiveView
  """
  require Logger
  use LogflareWeb, :live_view

  alias LogflareWeb.VercelLogDrainsView
  alias Logflare.Users
  alias Logflare.Vercel
  alias LogflareWeb.Router.Helpers, as: Routes

  @impl true
  def mount(_params, %{"user_id" => user_id}, socket) do
    if connected?(socket) do
      # Subscribe to Vercel webhook here.
    end

    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_vercel_auths()

    auths = user.vercel_auths |> Enum.sort_by(& &1.inserted_at, {:desc, NaiveDateTime})

    drains =
      for a <- auths do
        {:ok, resp} =
          Vercel.Client.new(a)
          |> Vercel.Client.list_log_drains()

        %{auth: a.access_token, inserted_at: a.inserted_at, resp: resp.body}
      end
      |> Enum.sort_by(& &1.inserted_at, {:desc, NaiveDateTime})

    socket =
      socket
      |> assign(:user, user)
      |> assign(:auths, auths)
      |> assign(:drains, drains)

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    VercelLogDrainsView.render("edit.html", assigns)
  end
end
