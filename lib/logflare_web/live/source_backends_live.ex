defmodule LogflareWeb.SourceBackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends

  def render(assigns) do
    ~L"""
    <div class="mt-4">
      <%= if !@show_create_form do %>
        <button class="btn btn-primary" phx-click="toggle-create-form">Add a backend</button>
      <% end %>
      <%= if @show_create_form do %>
      <%= f = form_for :source_backend, "#", [phx_submit: :save_source_backend, class: "mt-4"] %>
        Add <%= select f, :type, [:webhook], phx_change: :change_create_form_type %> backend
        <div class="form-group mt-2">
          <%= for f_config <- inputs_for(f, :config) do %>
            <%= case @create_form_type do %>
              <% "webhook" -> %>
                <%= label f_config, :url %>
                <%= text_input f_config, :url %>
            <% end %>
          <% end %>
        </div>
        <button type="button" class="btn btn-secondary" phx-click="toggle-create-form">Cancel</button>
        <%= submit "Add", class: "btn btn-primary" %>
      </form>
      <% end %>

      <div>
        Backends:
        <ul>
          <%= for sb <- @source_backends do %>
            <li>
                <%= sb.type %> <button class="ml-2 btn btn-danger" phx-click="remove_source_backend" phx-value-id="<%= sb.id %>">Remove</button>
                <%= case sb.type do %>
                <% :webhook -> %>
                  <ul>
                    <li>url: <%= sb.config.url %></li>
                  </ul>
              <% end %>
            </li>
          <% end %>
        </ul>
      </div>

    </div>
    """
  end

  def mount(_params, %{"source_id" => source_id}, socket) do
    source = Logflare.Sources.get(source_id)
    source_backends = Logflare.Backends.list_source_backends(source)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:source_backends, source_backends)
      |> assign(:show_create_form, false)
      |> assign(:create_form_type, "webhook")
      |> assign(
        :create_changeset,
        Logflare.Backends.SourceBackend.changeset(%Logflare.Backends.SourceBackend{}, %{
          source_id: source_id,
          type: "webhook",
          config: %{}
        })
      )

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event(
        "toggle-create-form",
        _,
        %{assigns: %{show_create_form: show_create_form}} = socket
      ) do
    {:noreply, assign(socket, show_create_form: !show_create_form)}
  end

  def handle_event(
        "save_source_backend",
        %{"source_backend" => params},
        %{assigns: %{source: source}} = socket
      ) do
    socket =
      case Logflare.Backends.create_source_backend(source, params["type"], params["config"]) do
        {:ok, _} ->
          socket
          |> assign(:show_create_form, false)

        {:error, changeset} ->
          # TODO: move this to a helper function
          message =
            Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
              Enum.reduce(opts, msg, fn {key, value}, acc ->
                String.replace(acc, "%{#{key}}", _to_string(value))
              end)
            end)
            |> Enum.reduce("", fn {k, v}, acc ->
              joined_errors = Enum.join(v, ";\n")
              "#{acc} #{k}: #{joined_errors}"
            end)

          socket
          |> put_flash(:error, "Encountered error when adding backend:\n#{message}")
      end

    socket =
      socket
      |> assign(:source_backends, Logflare.Backends.list_source_backends(source))

    {:noreply, socket}
  end

  def handle_event("change_create_form_type", %{"type" => type}, socket) do
    {:noreply, assign(socket, create_form_type: type)}
  end

  def handle_event("remove_source_backend", %{"id" => id}, %{assigns: %{source: source}} = socket) do
    Logger.debug("Removing source backend id: #{id}")
    source_backend = Backends.get_source_backend(id)
    Backends.delete_source_backend(source_backend)

    socket =
      socket
      |> put_flash(:info, "Successfully deleted backend of type #{source_backend.type}")
      |> assign(:source_backends, Backends.list_source_backends(source))

    {:noreply, socket}
  end

  defp _to_string(val) when is_list(val) do
    Enum.join(val, ", ")
  end

  defp _to_string(val), do: to_string(val)
end
