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

    socket =
      socket
      |> assign_user(user_id)
      |> assign_auths()

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"configurationId" => config_id} = params, _uri, socket) do
    contacting_vercel()
    send(self(), {:handle_params, params})

    {:noreply, assign_default_socket(socket)}
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    contacting_vercel()
    send(self(), :init_socket)

    {:noreply, assign_default_socket(socket)}
  end

  def handle_event("select_auth", params, socket) do
    contacting_vercel()

    send(self(), {:select_auth, params})

    {:noreply, socket}
  end

  def handle_event("validate", params, socket) do
    {:noreply, socket}
  end

  def handle_event(
        "create_drain",
        %{
          "fields" => %{
            "name" => name,
            "project" => project_id,
            "source" => source_token
          }
        },
        socket
      ) do
    auth = socket.assigns.selected_auth
    api_key = socket.assigns.user.api_key
    url = "https://api.logflare.app/logs/vercel?api_key=#{api_key}&source=#{source_token}"

    drain_params =
      if project_id == "all_projects",
        do: %{name: name, type: "json", url: url},
        else: %{name: name, type: "json", url: url, projectId: project_id}

    {:ok, resp} = Vercel.Client.new(auth) |> Vercel.Client.create_log_drain(drain_params)

    socket =
      case resp do
        %Tesla.Env{status: 200} ->
          socket
          |> assign_drains()
          |> assign_mapped_drains_sources()
          |> clear_flash()
          |> put_flash(:info, "Log drain created!")

        _ ->
          socket
          |> clear_flash()
          |> put_flash(:error, "Something went wrong. Please try again!")
      end

    {:noreply, socket}
  end

  def handle_event("delete_drain", %{"id" => drain_id}, socket) do
    {:ok, resp} =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.delete_log_drain(drain_id)

    socket =
      case resp do
        %Tesla.Env{status: 204} ->
          socket
          |> clear_flash()
          |> assign_drains()
          |> assign_mapped_drains_sources()
          |> put_flash(:info, "Log drain deleted!")

        %Tesla.Env{status: 404} ->
          socket
          |> clear_flash()
          |> put_flash(:info, "Log drain not found!")

        resp ->
          socket
          |> clear_flash()
          |> put_flash(:error, "Something went wrong. Please try again!")
      end

    {:noreply, socket}
  end

  def handle_event("delete_auth", %{"id" => auth_id}, socket) do
    socket =
      case Vercel.get_auth!(auth_id) |> Vercel.delete_auth() do
        {:ok, _resp} -> put_flash(socket, :info, "Integration deleted!")
        {:error, _resp} -> put_flash(socket, :error, "Something went wrong!")
      end

    user_id = socket.assigns.user.id

    socket =
      socket
      |> assign_user(user_id)
      |> assign_auths()
      |> assign_selected_auth()
      |> assign_drains()
      |> assign_mapped_drains_sources()

    {:noreply, socket}
  end

  def handle_info({:select_auth, %{"fields" => %{"installation" => auth_id}}}, socket) do
    auth = Vercel.get_auth!(auth_id)

    send_clear_flash()

    socket =
      socket
      |> assign_selected_auth(auth)
      |> assign_drains()
      |> assign_projects()
      |> assign_mapped_drains_sources()

    {:noreply, socket}
  end

  def handle_info({:handle_params, %{"configurationId" => config_id}}, socket) do
    auth = Vercel.get_auth_by(installation_id: config_id)

    socket =
      socket
      |> assign_selected_auth(auth)
      |> assign_drains()
      |> assign_projects()
      |> assign_mapped_drains_sources()
      |> clear_flash()

    {:noreply, socket}
  end

  def handle_info(:contacting_vercel, socket) do
    {:noreply, put_flash(socket, :info, "Contacting Vercel...")}
  end

  def handle_info(:clear_flash, socket) do
    {:noreply, clear_flash(socket)}
  end

  def handle_info(:init_socket, socket) do
    socket =
      socket
      |> assign_auths()
      |> assign_selected_auth()
      |> assign_drains()
      |> assign_projects()
      |> assign_mapped_drains_sources()
      |> clear_flash()

    {:noreply, socket}
  end

  defp assign_default_socket(socket) do
    socket
    |> assign(:mapped_drains_sources, [])
    |> assign(:projects, [])
    |> assign(:selected_auth, %Vercel.Auth{})
  end

  defp assign_drains(socket) do
    {:ok, drains} =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.list_log_drains()

    drains =
      drains
      |> case do
        %Tesla.Env{status: 200} = resp ->
          resp.body
          |> Enum.sort_by(& &1["createdAt"])

        resp ->
          []
      end

    socket
    |> assign(:drains, drains)
  end

  defp assign_mapped_drains_sources(socket) do
    drains = socket.assigns.drains
    sources = socket.assigns.user.sources

    mapped_drains_sources =
      Enum.map(drains, fn d ->
        uri = URI.parse(d["url"])
        params = URI.decode_query(uri.query)
        source_token = params["source"]

        source = Enum.find(sources, &(Atom.to_string(&1.token) == source_token))

        %{drain: d, source: source}
      end)

    assign(socket, :mapped_drains_sources, mapped_drains_sources)
  end

  defp assign_projects(socket) do
    {:ok, resp} =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.list_projects()

    projects =
      case resp do
        %Tesla.Env{status: 403} ->
          []

        resp ->
          Enum.map(resp.body["projects"], &{&1["name"], &1["id"]})
      end

    assign(socket, :projects, projects)
  end

  defp assign_user(socket, user_id) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_vercel_auths()

    assign(socket, :user, user)
  end

  defp assign_selected_auth(socket) do
    auths = socket.assigns.auths

    selected =
      case auths do
        [] -> %Vercel.Auth{}
        auths -> hd(auths)
      end

    assign_selected_auth(socket, selected)
  end

  defp assign_selected_auth(socket, nil) do
    assign_selected_auth(socket)
  end

  defp assign_selected_auth(socket, %Vercel.Auth{} = auth) do
    assign(socket, :selected_auth, auth)
  end

  defp assign_auths(socket) do
    user = socket.assigns.user
    auths = user.vercel_auths |> Enum.sort_by(& &1.inserted_at, {:desc, NaiveDateTime})

    assign(socket, :auths, auths)
  end

  defp send_clear_flash() do
    send(self, :clear_flash)
  end

  defp contacting_vercel() do
    send(self, :contacting_vercel)
  end

  @impl true
  def render(assigns) do
    VercelLogDrainsView.render("index.html", assigns)
  end
end
