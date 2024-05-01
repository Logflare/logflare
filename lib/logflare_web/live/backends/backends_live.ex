defmodule LogflareWeb.BackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends
  alias Logflare.Users

  embed_templates("actions/*", suffix: "_action")
  embed_templates("components/*")

  def render(%{live_action: :index} = assigns), do: index_action(assigns)
  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  def mount(params, %{"user_id" => user_id}, socket) do
    {user_id, _} =
      case user_id do
        v when is_binary(v) -> Integer.parse(v)
        _ -> {user_id, nil}
      end

    user = Users.get(user_id)

    socket =
      socket
      |> assign(:user, user)
      |> assign(:backends, [])
      |> assign(:backend, nil)
      |> refresh_backends()
      |> refresh_backend(params["id"])
      |> assign(:create_form_type, nil)

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_params(params, uri, socket) do
    socket =
      socket
      |> refresh_backends()
      |> refresh_backend(params["id"])

    {:noreply, socket}
  end

  def handle_event("toggle-create-form", _, socket) do
    %{assigns: %{toggle_rule_form: toggle_rule_form}} = socket
    {:noreply, assign(socket, toggle_rule_form: !toggle_rule_form)}
  end

  def handle_event("save_backend", %{"backend" => params}, socket) do
    socket =
      case Logflare.Backends.create_backend(params) do
        {:ok, backend} ->
          socket
          |> assign(:toggle_rule_form, false)
          |> assign(:backends, [backend | socket.assigns.backends])
          |> put_flash(:info, "Successfully created backend")
          |> push_patch(to: ~p"/backends/#{backend.id}")

        {:error, changeset} ->
          # TODO: move this to a helper function
          message = changeset_to_flash_message(changeset)

          put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
      end

    socket = refresh_backends(socket)

    {:noreply, socket}
  end

  def handle_event("change_create_form_type", %{"backend" => %{"type" => type}}, socket) do
    {:noreply, assign(socket, create_form_type: type)}
  end

  def handle_event("delete", %{"backend_id" => id}, %{assigns: assigns} = socket) do
    Logger.debug("Removing backend id: #{id}")
    backend = Backends.get_backend(id)

    with {:ok, _backend} <- Backends.delete_backend(backend) do
      socket =
        socket
        |> put_flash(:info, "Successfully deleted backend of type #{backend.type}")
        |> refresh_backends()
        |> push_patch(to: ~p"/backends")

      {:noreply, socket}
    else
      {:error, changeset} ->
        message = changeset_to_flash_message(changeset)

        {:noreply,
         put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")}
    end
  end

  defp _to_string(val) when is_list(val) do
    Enum.join(val, ", ")
  end

  defp _to_string(val), do: to_string(val)

  defp changeset_to_flash_message(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", _to_string(value))
      end)
    end)
    |> Enum.reduce("", fn {k, v}, acc ->
      joined_errors = Enum.join(v, ";\n")
      "#{acc} #{k}: #{joined_errors}"
    end)
  end

  defp refresh_backends(socket) do
    backends =
      Backends.list_backends_by_user_id(socket.assigns.user.id)
      |> Backends.preload_rules()

    socket
    |> assign(:backends, backends)
  end

  defp refresh_backend(socket, nil), do: socket

  defp refresh_backend(socket, id) do
    assign(socket, :backend, Backends.get_backend(id) |> Backends.preload_rules())
  end
end
