defmodule LogflareWeb.AccessTokensLive do
  @moduledoc false
  use LogflareWeb, :live_view

  require Logger

  alias Logflare.Auth
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Teams.TeamContext

  def render(assigns) do
    assigns =
      assigns
      |> assign(:scopes, [
        {
          "ingest",
          "For ingestion into a source. Allows ingest into all sources if no specific source is selected."
        },
        {
          "query",
          "For querying an endpoint. Allows querying of all endpoints if no specific endpoint is selected"
        },
        {
          "private",
          "Create and modify account resources"
        },
        if(Auth.can_create_admin_token?(assigns.team_context),
          do: {Auth.admin_scope(), "Create and modify account resources and team users."}
        )
      ])

    ~H"""
    <.subheader>
      <:path>
        ~/accounts/<.subheader_path_link live_patch to={~p"/access-tokens"} team={@team}>access tokens</.subheader_path_link>
      </:path>
      <.subheader_link to="https://docs.logflare.app/concepts/access-tokens/" external={true} text="docs" fa_icon="book" />
    </.subheader>

    <section class="content container mx-auto tw-flex tw-flex-col w-full tw-gap-4">
      <div>
        <button class="btn btn-primary" phx-click="toggle-create-form" phx-value-show="true">
          Create access token
        </button>
      </div>
      <div>
        <p style="white-space: pre-wrap">There are 3 ways of authenticating with the API: in the <code>Authorization</code> header, the <code>X-API-KEY</code> header, or the <code>api_key</code> query parameter.

          The <code>Authorization</code> header method expects the header format <code>Authorization: Bearer your-access-token</code>.
          The <code>X-API-KEY</code> header method expects the header format <code>X-API-KEY: your-access-token</code>.
          The <code>api_key</code> query parameter method expects the search format <code>?api_key=your-access-token</code>.</p>

        <.form for={@create_token_form} action="#" phx-change="update-token-form" phx-submit="create-token" class={["mt-4", "jumbotron jumbotron-fluid tw-p-4", if(@show_create_form == false, do: "hidden")]}>
          <h5>New Access Token</h5>
          <div class="form-group">
            <label name="description">Description</label>
            <input name="description" autofocus class="form-control" value={@create_token_form["description"]} />
            <small class="form-text text-muted">A short description for identifying what this access token is to be used for.</small>
          </div>

          <div class="form-group ">
            <label name="scopes" class="tw-mr-3">Scope</label>
            <.scope_input :for={{value, description} <- @scopes} endpoints={@endpoints} sources={@sources} value={value} description={description} form={@create_token_form} />
          </div>
          <button type="button" class="btn btn-secondary" phx-click="toggle-create-form" phx-value-show="false">Cancel</button>
          {submit("Create", class: "btn btn-primary")}
        </.form>

        <%= if @created_token do %>
          <.alert variant="success">
            <p>Access token created successfully, copy this token to a safe location. For security purposes, this token will not be shown again.</p>

            <pre class="p-2"><%= @created_token.token %></pre>
            <button class="btn btn-secondary" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: @created_token.token})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
              <i class="fa fa-clone" aria-hidden="true"></i> Copy
            </button>
            <button class="btn btn-secondary" phx-click="dismiss-created-token">
              Dismiss
            </button>
          </.alert>
        <% end %>
      </div>

      <%= if @access_tokens == [] do %>
        <.alert variant="dark" class="tw-max-w-md">
          <h5>Legacy Ingest API Key</h5>
          <p><strong>Deprecated</strong>, use access tokens instead.</p>
          <button class="btn btn-secondary btn-sm" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: @user.api_key})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
            <i class="fa fa-clone" aria-hidden="true"></i> Copy
          </button>
        </.alert>
      <% end %>

      <table class={["table-dark", "table-auto", "w-full", "flex-grow", if(@access_tokens == [], do: "hidden")]}>
        <thead>
          <tr>
            <th class="p-2">Description</th>
            <th class="p-2">Scope</th>
            <th class="p-2">Created on</th>
            <th class="p-2">Actions</th>
          </tr>
        </thead>
        <tbody>
          <%= for token <- @access_tokens do %>
            <tr>
              <td class="p-2">
                <span class="tw-text-sm">
                  <%= if token.description do %>
                    {token.description}
                  <% else %>
                    <span class="tw-italic">No description</span>
                  <% end %>
                </span>
              </td>
              <td>
                <span :for={scope <- String.split(token.scopes || "")} class="badge badge-secondary mr-1">
                  {case scope do
                    "ingest" <> _ -> get_ingest_label(assigns, scope)
                    "query" <> _ -> get_query_label(assigns, scope)
                    scope -> scope
                  end}
                </span>
              </td>
              <td class="p-2 tw-text-sm">
                {Calendar.strftime(token.inserted_at, "%d %b %Y, %I:%M:%S %p")}
              </td>

              <td class="p-2">
                <button :if={!(token.scopes =~ "private")} class="btn btn-secondary btn-sm" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: token.token})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
                  <i class="fa fa-clone" aria-hidden="true"></i> Copy
                </button>
                <button class="btn text-danger btn-sm" data-confirm="Are you sure? This cannot be undone." phx-click="revoke-token" phx-value-token-id={token.id} data-toggle="tooltip" data-placement="top" title="Revoke access token forever">
                  <i class="fa fa-trash" aria-hidden="true"></i> Revoke
                </button>
              </td>
            </tr>
          <% end %>
        </tbody>
      </table>
    </section>
    """
  end

  attr :sources, :list
  attr :endpoints, :list
  attr :value, :string
  attr :description, :string
  attr :form, Phoenix.HTML.Form

  def scope_input(assigns) do
    assigns =
      assigns
      |> assign_new(:title, fn
        %{value: "private:admin"} -> "Admin"
        %{value: value} -> String.capitalize(value)
      end)

    ~H"""
    <div class="form-check tw-mr-2">
      <input class="form-check-input" type="checkbox" name="scopes_main[]" id={["scopes", "main", @value]} value={@value} checked={@value in @form["scopes_main"]} />
      <label class="form-check-label tw-px-1" for={["scopes", "main", @value]}>
        {@title}
        <small class="form-text text-muted">{@description}</small>
        <select :for={input_n <- 0..2} :if={@value == "ingest" and @value in @form["scopes_main"]} id={["scopes", "ingest", input_n]} name="scopes_ingest[]" class="mt-1 form-control form-control-sm">
          <option hidden value="">Ingest into a specific source...</option>
          <option :for={source <- @sources} selected={"ingest:source:#{source.id}" == Enum.at(@form["scopes_ingest"], input_n)} value={"ingest:source:#{source.id}"} }>Ingest into {source.name} only</option>
        </select>
        <select :for={input_n <- 0..2} :if={@value == "query" and @value in @form["scopes_main"]} id={["scopes", "query", input_n]} name="scopes_query[]" class="mt-1 form-control form-control-sm">
          <option hidden value="">Query a specific endpoint...</option>
          <option :for={endpoint <- @endpoints} value={"query:endpoint:#{endpoint.id}"} selected={"query:endpoint:#{endpoint.id}" == Enum.at(@form["scopes_query"], input_n)}>Query {endpoint.name} only</option>
        </select>
      </label>
    </div>
    """
  end

  @default_create_form %{
    "description" => "",
    "scopes" => [],
    "scopes_ingest" => [],
    "scopes_query" => [],
    "scopes_main" => ["ingest"]
  }
  def mount(_params, _session, socket) do
    %{assigns: %{user: user}} = socket
    sources = Sources.list_sources_by_user(user)
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)
    team_context = struct(TeamContext, socket.assigns)

    socket =
      socket
      |> assign(:show_create_form, false)
      |> assign(:created_token, nil)
      |> assign(:sources, sources)
      |> assign(:endpoints, endpoints)
      |> assign(scopes_ingest_sources: %{})
      |> assign(scopes_query_endpoints: %{})
      |> assign(create_token_form: @default_create_form)
      |> assign(:team_context, team_context)
      |> do_refresh()

    {:ok, socket}
  end

  def handle_event("toggle-create-form", %{"show" => value}, socket)
      when value in ["true", "false"],
      do: {:noreply, assign(socket, show_create_form: value === "true")}

  def handle_event("dismiss-created-token", _params, socket) do
    {:noreply, assign(socket, created_token: nil)}
  end

  def handle_event(
        "create-token",
        params,
        %{assigns: %{user: user}} = socket
      ) do
    Logger.debug(
      "Creating access token for user, user_id=#{inspect(user.id)}, params: #{inspect(params)}"
    )

    scopes_ingest_params = (Map.get(params, "scopes_ingest") || []) |> Enum.filter(&(&1 != ""))
    scopes_query_params = (Map.get(params, "scopes_query") || []) |> Enum.filter(&(&1 != ""))
    scopes_main_params = (Map.get(params, "scopes_main") || []) |> Enum.filter(&(&1 != ""))

    scopes_main =
      if scopes_ingest_params != [],
        do: List.delete(scopes_main_params, "ingest"),
        else: scopes_main_params

    scopes_main =
      if scopes_query_params != [], do: List.delete(scopes_main, "query"), else: scopes_main

    scopes = scopes_main ++ scopes_ingest_params ++ scopes_query_params

    attrs =
      Map.take(params, ["description"])
      |> Map.put("scopes", Enum.join(scopes, " "))

    {:ok, token} = Auth.create_access_token(user, attrs)

    socket =
      socket
      |> do_refresh()
      |> assign(:show_create_form, false)
      |> assign(:create_token_form, @default_create_form)
      |> assign(:created_token, token)

    {:noreply, socket}
  end

  def handle_event(
        "update-token-form",
        payload,
        socket
      ) do
    data = Map.drop(payload, ["_csrf_token", "_target"])

    data =
      if "ingest" in Map.get(data, "scopes_main", []) do
        data
      else
        Map.put(data, "scopes_ingest", [])
      end

    data =
      if "query" in Map.get(data, "scopes_main", []) do
        data
      else
        Map.put(data, "scopes_query", [])
      end

    merged = Map.merge(socket.assigns.create_token_form, data)

    {:noreply, assign(socket, :create_token_form, merged)}
  end

  def handle_event(
        "revoke-token",
        %{"token-id" => token_id},
        %{assigns: %{access_tokens: tokens}} = socket
      ) do
    token = Enum.find(tokens, &("#{&1.id}" == token_id))
    Logger.debug("Revoking access token")
    :ok = Auth.revoke_access_token(token)

    socket =
      socket
      |> do_refresh()

    {:noreply, socket}
  end

  defp do_refresh(%{assigns: %{user: user}} = socket) do
    tokens = user |> Auth.list_valid_access_tokens() |> Enum.sort_by(& &1.inserted_at, :desc)

    scopes_ingest_sources =
      for token <- tokens,
          str_id <- parse_ingest_scope_source_id(token.scopes),
          source = Sources.get(str_id),
          into: socket.assigns.scopes_ingest_sources do
        {str_id, source}
      end

    scopes_query_endpoints =
      for token <- tokens,
          str_id <- parse_query_scope_endpoint_id(token.scopes),
          endpoint = Endpoints.get_endpoint_query(str_id),
          into: socket.assigns.scopes_query_endpoints do
        {str_id, endpoint}
      end

    socket
    |> assign(access_tokens: tokens)
    |> assign(scopes_ingest_sources: scopes_ingest_sources)
    |> assign(scopes_query_endpoints: scopes_query_endpoints)
    |> assign(created_token: nil)
  end

  # get list of string ids from scopes string
  defp parse_ingest_scope_source_id(scopes) do
    Regex.scan(~r/ingest:source:([0-9]+)/, scopes, capture: :all_but_first)
    |> List.flatten()
  end

  # get list of string ids from scopes string
  defp parse_query_scope_endpoint_id(scopes) do
    Regex.scan(~r/query:endpoint:([0-9]+)/, scopes, capture: :all_but_first)
    |> List.flatten()
  end

  defp get_query_label(_assigns, "query"), do: "query (all)"

  defp get_query_label(%{scopes_query_endpoints: endpoint_map}, "query:endpoint:" <> str_id) do
    if endpoint = Map.get(endpoint_map, str_id) do
      "query (#{endpoint.name})"
    else
      "query (deleted)"
    end
  end

  defp get_ingest_label(_assigns, "ingest"), do: "ingest (all)"

  defp get_ingest_label(%{scopes_ingest_sources: source_map}, "ingest:source:" <> str_id) do
    if source = Map.get(source_map, str_id) do
      "ingest (#{source.name})"
    else
      "ingest (deleted)"
    end
  end
end
