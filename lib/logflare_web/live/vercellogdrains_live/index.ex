defmodule LogflareWeb.VercelLogDrainsLive do
  @moduledoc """
  Vercel Log Drain edit LiveView
  """
  require Logger
  use LogflareWeb, :live_view

  alias LogflareWeb.VercelLogDrainsView
  alias Logflare.Users
  alias Logflare.Vercel

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
  def handle_params(%{"configurationId" => _config_id} = params, _uri, socket) do
    contacting_vercel()
    send(self(), {:init_socket, params})

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

  def handle_event("validate", _params, socket) do
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

    resp = Vercel.Client.new(auth) |> Vercel.Client.create_log_drain(drain_params)

    socket =
      case resp do
        {:ok, %Tesla.Env{status: 200}} ->
          socket
          |> assign_drains()
          |> assign_mapped_drains_sources_projects()
          |> clear_flash()
          |> put_flash(:info, "Log drain created!")

        {:ok, %Tesla.Env{status: 400}} ->
          socket
          |> clear_flash()
          |> put_flash(:info, "Please use alphanumeric characters for your log drain name!")

        {:ok, %Tesla.Env{status: 403}} ->
          unauthorized_socket(socket)

        {:error, "Encounter Mint error %Mint.TransportError{reason: :timeout}"} ->
          handle_timeout_socket(socket, resp)

        {:error, resp} ->
          unknown_error_socket(socket, resp)
      end

    {:noreply, socket}
  end

  def handle_event(
        "create_drain",
        %{
          "fields" => %{
            "name" => _name,
            "project" => _project_id
          }
        },
        socket
      ) do
    socket =
      socket
      |> clear_flash()
      |> put_flash(:error, "Please select a Logflare source!")

    {:noreply, socket}
  end

  def handle_event("delete_drain", %{"id" => drain_id}, socket) do
    resp =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.delete_log_drain(drain_id)

    socket =
      case resp do
        {:ok, %Tesla.Env{status: 204}} ->
          socket
          |> clear_flash()
          |> assign_drains()
          |> assign_mapped_drains_sources_projects()
          |> put_flash(:info, "Log drain deleted!")

        {:ok, %Tesla.Env{status: 404}} ->
          socket
          |> clear_flash()
          |> put_flash(:info, "Log drain not found!")

        {:ok, %Tesla.Env{status: 403}} ->
          unauthorized_socket(socket)

        {:error, "Encounter Mint error %Mint.TransportError{reason: :timeout}"} ->
          handle_timeout_socket(socket, resp)

        {:error, resp} ->
          unknown_error_socket(socket, resp)
      end

    {:noreply, socket}
  end

  def handle_event("delete_auth", %{"id" => auth_id}, socket) do
    user_id = socket.assigns.user.id

    socket =
      case Vercel.get_auth!(auth_id) |> Vercel.delete_auth() do
        {:ok, _resp} ->
          socket
          |> assign_user(user_id)
          |> assign_auths()
          |> init_socket()
          |> put_flash(:info, "Integration deleted!")

        {:error, resp} ->
          unknown_error_socket(socket, resp)
      end

    {:noreply, socket}
  end

  def handle_info({:select_auth, %{"fields" => %{"installation" => auth_id}}}, socket) do
    auth = Vercel.get_auth!(auth_id)

    {:noreply, init_socket(socket, auth)}
  end

  def handle_info({:init_socket, %{"configurationId" => config_id}}, socket) do
    auth = Vercel.get_auth_by(installation_id: config_id)

    socket =
      if auth do
        init_socket(socket, auth)
      else
        send_flash(
          :error,
          "ConfigurationId `#{config_id}` not found in your Logflare account. Please reinstall the Loglfare Vercel integration to manage your log drains here."
        )

        socket
        |> init_socket()
      end

    {:noreply, socket}
  end

  def handle_info(:init_socket, socket) do
    auth =
      case socket.assigns.auths do
        [] -> %Vercel.Auth{}
        auths -> hd(auths)
      end

    {:noreply, init_socket(socket, auth)}
  end

  def handle_info(:contacting_vercel, socket) do
    {:noreply, put_flash(socket, :info, "Contacting Vercel...")}
  end

  def handle_info(:clear_flash, socket) do
    {:noreply, clear_flash(socket)}
  end

  def handle_info({:send_flash, {level, message}}, socket) do
    {:noreply, put_flash(socket, level, message)}
  end

  defp init_socket(socket, auth \\ %Vercel.Auth{})

  defp init_socket(socket, nil) do
    init_socket(socket, %Vercel.Auth{})
  end

  defp init_socket(socket, %Vercel.Auth{} = auth) do
    auth =
      cond do
        auth.installation_id ->
          auth

        Enum.at(socket.assigns.auths, 0) ->
          Enum.at(socket.assigns.auths, 0)

        true ->
          auth
      end

    socket
    |> assign_selected_auth(auth)
    |> assign_drains()
    |> assign_projects()
    |> assign_mapped_drains_sources_projects()
    |> assign_mapped_auths_teams()
  end

  defp assign_default_socket(socket) do
    socket
    |> assign(:mapped_drains_sources, [])
    |> assign(:projects, [])
    |> assign(:selected_auth, %Vercel.Auth{})
  end

  defp assign_drains(socket) do
    resp =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.list_log_drains()

    case resp do
      {:ok, %Tesla.Env{status: 200} = resp} ->
        drains =
          resp.body
          |> Enum.sort_by(& &1["createdAt"])

        socket
        |> assign(:drains, drains)
        |> clear_flash()

      {:ok, %Tesla.Env{status: 403}} ->
        socket
        |> assign(:drains, [])

      {:error, "Encounter Mint error %Mint.TransportError{reason: :timeout}"} ->
        handle_timeout_socket(socket, resp)

      {:error, resp} ->
        socket
        |> assign(:drains, [])
        |> unknown_error_socket(resp)
    end
  end

  defp assign_mapped_drains_sources_projects(socket) do
    drains = socket.assigns.drains
    sources = socket.assigns.user.sources
    projects = socket.assigns.projects

    mapped_drains_sources =
      Enum.map(drains, fn d ->
        uri = URI.parse(d["url"])
        params = URI.decode_query(uri.query)
        source_token = params["source"]

        source = Enum.find(sources, &(Atom.to_string(&1.token) == source_token))
        project = Enum.find(projects, &(&1["id"] == d["projectId"]))

        %{drain: d, source: source, project: project}
      end)

    assign(socket, :mapped_drains_sources, mapped_drains_sources)
  end

  defp assign_projects(socket) do
    resp =
      Vercel.Client.new(socket.assigns.selected_auth)
      |> Vercel.Client.list_projects()

    case resp do
      {:ok, %Tesla.Env{status: 200} = resp} ->
        projects = resp.body["projects"]

        socket
        |> assign(:projects, projects)
        |> clear_flash()

      {:ok, %Tesla.Env{status: 403}} ->
        socket

      {:error, "Encounter Mint error %Mint.TransportError{reason: :timeout}"} ->
        handle_timeout_socket(socket, resp)

      {:error, resp} ->
        unknown_error_socket(socket, resp)
    end
  end

  defp assign_user(socket, user_id) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_vercel_auths()

    assign(socket, :user, user)
  end

  defp assign_selected_auth(socket, %Vercel.Auth{id: id} = auth) when is_nil(id) do
    socket
    |> assign(:selected_auth, auth)
    |> clear_flash()
  end

  defp assign_selected_auth(socket, %Vercel.Auth{id: id} = auth) when is_integer(id) do
    resp =
      Vercel.Client.new(auth)
      |> Vercel.Client.get_user()

    case resp do
      {:ok, %Tesla.Env{status: 200}} ->
        socket
        |> assign(:selected_auth, auth)

      {:ok, %Tesla.Env{status: 403}} ->
        socket
        |> assign(:selected_auth, auth)
        |> unauthorized_socket()

      {:ok, %Tesla.Env{status: 400}} ->
        Logger.error("Bad Vercel user API request.", error_string: inspect(resp))
        socket

      {:error, "Encounter Mint error %Mint.TransportError{reason: :timeout}"} ->
        handle_timeout_socket(socket, resp)

      resp ->
        Logger.error("Unknown Vercel API error.", error_string: inspect(resp))
        socket
    end
  end

  defp assign_auths(socket) do
    user = socket.assigns.user
    auths = user.vercel_auths |> Enum.sort_by(& &1.inserted_at, {:desc, NaiveDateTime})

    assign(socket, :auths, auths)
  end

  defp assign_mapped_auths_teams(socket) do
    auths = socket.assigns.auths

    auths_teams =
      Enum.map(auths, fn auth ->
        team =
          if auth.team_id do
            resp =
              Vercel.Client.new(auth)
              |> Vercel.Client.get_team(auth.team_id)

            case resp do
              {:ok, %Tesla.Env{status: 200} = resp} ->
                resp.body

              {:ok, %Tesla.Env{status: 403}} ->
                nil

              _ ->
                nil
            end
          end

        %{auth: auth, team: team}
      end)

    assign(socket, :auths_teams, auths_teams)
  end

  defp send_flash(level, message) do
    send(self(), {:send_flash, {level, message}})
  end

  defp contacting_vercel do
    send(self(), :contacting_vercel)
  end

  defp unauthorized_socket(socket) do
    socket
    |> clear_flash()
    |> put_flash(
      :error,
      "This installation is not authorized. Try reinstalling the Logflare integration."
    )
  end

  defp unknown_error_socket(socket, resp) do
    Logger.error("Unknown Vercel API error.", error_string: inspect(resp))

    socket
    |> clear_flash()
    |> put_flash(
      :error,
      "Something went wrong. Try reinstalling the Logflare integration. Contact support if this continues."
    )
  end

  defp handle_timeout_socket(socket, resp) do
    Logger.error("Vercel timeout!", error_string: inspect(resp))

    socket
    |> clear_flash()
    |> put_flash(:error, "Vercel API timeout! Please try again.")
  end

  @impl true
  def render(assigns) do
    VercelLogDrainsView.render("index.html", assigns)
  end
end
