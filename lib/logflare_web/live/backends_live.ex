defmodule LogflareWeb.BackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends

  def render(assigns) do
    ~H"""
    <div class="my-4 container">
      <%= if !@show_create_form do %>
        <button class="btn btn-primary" phx-click="toggle-create-form">Add a backend</button>
      <% else %>
        <.form :let={f} for={%{}} as={:backend} action="#" phx-submit="save_backend" class="mt-4">
          <p>Add backend</p>
          <%= select(f, :type, ["Select a backend type...", Webhook: :webhook, Postgres: :postgres, BigQuery: :bigquery],
            phx_change: :change_create_form_type,
            class: "form-control form-control-margin",
            id: "type"
          ) %>

          <div class="form-group mt-2">
            <%= hidden_input(f, :user_id, value: @user.id) %>
            <%= label(f, :name) %>
            <%= text_input(f, :name, class: "form-control") %>
            <%= label(f, :description) %>
            <%= text_input(f, :description, class: "form-control") %>
          </div>

          <.inputs_for :let={f_config} field={f[:config]}>
            <div class="form-group mt-2">
              <%= case @create_form_type do %>
                <% "webhook" -> %>
                  <div class="form-group">
                    <%= label f_config, :url do %>
                      Websocket URL to send the ingested data
                    <% end %>
                    <%= text_input(f_config, :url, class: "form-control") %>
                  </div>
                <% "postgres" -> %>
                  <div class="form-group">
                    <%= label f_config, :url do %>
                      Postgres URL for the ingestion database
                    <% end %>
                    <%= text_input(f_config, :url, class: "form-control") %>
                    <small class="form-text text-muted">
                      Postgres URL with the following format, for example: <code>postgresql://user:password@host:port/database</code>
                    </small>
                  </div>
                  <div class="form-group">
                    <%= label f_config, :schema do %>
                      Schema where data should be store, if blank the database defaults will be used
                    <% end %>
                    <%= text_input(f_config, :schema, class: "form-control") %>
                    <small class="form-text text-muted">
                      Schema name, for example: <code>analytics</code>
                    </small>
                  </div>
                <% "bigquery" -> %>
                  <div class="form-group">
                    <%= label(f_config, :project_id, "Google Cloud Project ID") %>
                    <%= text_input(f_config, :project_id, class: "form-control") %>
                    <small class="form-text text-muted">
                      The Google Cloud project ID where the data is to be inserted into via BigQuery.
                    </small>
                  </div>

                  <div class="form-group">
                    <%= label f_config, :dataset_id do %>
                      Dataset ID
                    <% end %>
                    <%= text_input(f_config, :dataset_id, class: "form-control") %>
                    <small class="form-text text-muted">
                      A BigQuery Dataset ID where data will be stored.
                    </small>
                  </div>
                <% _ -> %>
                  <div>Select a Backend Type</div>
              <% end %>
            </div>
          </.inputs_for>
          <button type="button" class="btn btn-secondary" phx-click="toggle-create-form">
            Cancel
          </button>
          <%= submit("Add", class: "btn btn-primary") %>
        </.form>
      <% end %>
      <div :if={not Enum.empty?(@backends)} class="tw-mt-10">
        Backends:
        <ul>
          <li :for={sb <- @backends}>
            <%= sb.name %>
            <%= sb.description %>
            <%= sb.type %>
            <button class="ml-2 btn btn-danger" phx-click="remove_backend" phx-value-id={sb.id}>
              Remove
            </button>
            <%= case sb.type do %>
              <% :webhook -> %>
                <ul>
                  <li>url: <%= sb.config.url %></li>
                </ul>
              <% :postgres -> %>
                <ul>
                  <li>url: <%= sb.config.url %> - schema: <%= Map.get(sb.config, :schema, "") %></li>
                </ul>
              <% :bigquery -> %>
                <ul>
                  <li>Project ID: <%= sb.config.project_id %></li>
                  <li>Dataset ID: <%= sb.config.dataset_id %></li>
                </ul>
            <% end %>
          </li>
        </ul>
      </div>
    </div>
    """
  end

  def mount(_params, %{"user_id" => user_id}, socket) do
    backends = Logflare.Backends.list_backends_by_user_id(user_id)
    user = Logflare.Users.get(user_id)

    socket =
      socket
      |> assign(:user, user)
      |> assign(:backends, backends)
      |> assign(:show_create_form, false)
      |> assign(:create_form_type, nil)

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event("toggle-create-form", _, socket) do
    %{assigns: %{show_create_form: show_create_form}} = socket
    {:noreply, assign(socket, show_create_form: !show_create_form)}
  end

  def handle_event("save_backend", %{"backend" => params}, socket) do
    socket =
      case Logflare.Backends.create_backend(params) do
        {:ok, backend} ->
          socket
          |> assign(:show_create_form, false)
          |> assign(:backends, [backend | socket.assigns.backends])

        {:error, changeset} ->
          # TODO: move this to a helper function
          message = changeset_to_flash_message(changeset)

          put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
      end

    socket =
      assign(
        socket,
        :backends,
        Logflare.Backends.list_backends_by_user_id(socket.assigns.user.id)
      )

    {:noreply, socket}
  end

  def handle_event("change_create_form_type", %{"backend" => %{"type" => type}}, socket) do
    {:noreply, assign(socket, create_form_type: type)}
  end

  def handle_event("remove_backend", %{"id" => id}, %{assigns: assigns} = socket) do
    Logger.debug("Removing backend id: #{id}")
    backend = Backends.get_backend(id)

    with {:ok, _backend} <- Backends.delete_backend(backend) do
      socket =
        socket
        |> put_flash(:info, "Successfully deleted backend of type #{backend.type}")
        |> assign(:backends, Backends.list_backends_by_user_id(assigns.user.id))

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
end
