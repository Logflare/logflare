defmodule LogflareWeb.AccessTokensLive do
  @moduledoc false
  use LogflareWeb, :live_view
  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]
  require Logger
  alias Logflare.Auth
  alias Logflare.Sources
  alias Logflare.Endpoints

  def render(assigns) do
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

        <.form :let={f} for={@create_token_form} action="#" phx-change="update-token-form" phx-submit="create-token" class={["mt-4", "jumbotron jumbotron-fluid tw-p-4", if(@show_create_form == false, do: "hidden")]}>
          <h5>New Access Token</h5>
          <div class="form-group">
            <label name="description">Description</label>
            <input name="description" autofocus class="form-control" value={f[:description].value} />
            <small class="form-text text-muted">A short description for identifying what this access token is to be used for.</small>
          </div>

          <div class="form-group ">
            <label name="scopes" class="tw-mr-3">Scope</label>
            <input type="hidden" name="scopes_main[]" value="" />
            <.scope value="ingest" label="Ingest into" description="Choose whether this token can ingest into all or selected sources." form={f} resource="sources" options={source_options(@sources)} mode_field={f[:scopes_ingest_mode]} selected_field={f[:scopes_ingest]} />
            <.scope value="query" label="Query" description="Choose whether this token can query all or selected endpoints." form={f} resource="endpoints" options={endpoint_options(@endpoints)} mode_field={f[:scopes_query_mode]} selected_field={f[:scopes_query]} />
            <.scope value="private" label="Private" description="For account management, has all privileges" form={f} />
          </div>
          <button type="button" class="btn btn-secondary" phx-click="toggle-create-form" phx-value-show="false">Cancel</button>
          {submit("Create", class: "btn btn-primary")}
        </.form>

        <%= if @created_token do %>
          <.alert variant="success">
            <p>Access token created successfully, copy this token to a safe location. For security purposes, this token will not be shown again.</p>

            <pre class="p-2"><%= @created_token.token %></pre>
            <.clipboard_button text={@created_token.token} />
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
          <.clipboard_button text={@user.api_key} class="btn-sm" />
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
                <.clipboard_button :if={!(token.scopes =~ "private")} text={token.token} class="btn-sm" />
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

  attr :value, :string, required: true
  attr :label, :string, required: true
  attr :description, :string, required: true
  attr :form, Phoenix.HTML.Form, required: true
  attr :resource, :string, default: nil
  attr :options, :list, default: []
  attr :mode_field, Phoenix.HTML.FormField, default: nil
  attr :selected_field, Phoenix.HTML.FormField, default: nil

  defp scope(assigns) do
    private_scope? = private_scope?(assigns.form[:private].value)
    resource_checked? = assigns.value in assigns.form[:scopes_main].value

    assigns =
      assigns
      |> assign(:private_scope?, private_scope?)
      |> assign(
        :checked?,
        if(assigns.value == "private",
          do: private_scope?,
          else: resource_checked? or private_scope?
        )
      )
      |> assign(:resource_checked?, resource_checked?)
      |> assign(:disabled?, private_scope? and assigns.value != "private")
      |> assign(
        :selected_values,
        if(assigns.selected_field, do: coerce_scope_list(assigns.selected_field.value), else: [])
      )

    ~H"""
    <div class="form-check tw-mr-2">
      <input :if={@value == "private"} type="hidden" name="private" value="false" />
      <input class="form-check-input" type="checkbox" name={if(@value == "private", do: "private", else: "scopes_main[]")} id={["scopes", "main", @value]} value={if(@value == "private", do: "true", else: @value)} checked={@checked?} disabled={@disabled?} />
      <label class="form-check-label tw-px-1" for={["scopes", "main", @value]}>
        {@label}
        <small class="form-text text-muted">{@description}</small>
      </label>
      <div :if={@resource && @checked?} id={"scopes-#{@value}-permissions-#{if(@private_scope?, do: "private", else: "standard")}"} class="tw-ml-5 tw-mt-2 tw-max-w-3xl">
        <input :if={@private_scope? and @resource_checked?} type="hidden" name="scopes_main[]" value={@value} />
        <input :if={@private_scope?} type="hidden" name={@mode_field.name} value={@mode_field.value} />
        <input :for={selected <- @selected_values} :if={@private_scope?} type="hidden" name={"#{@selected_field.name}[]"} value={selected} />
        <div class="form-check">
          <input class="form-check-input" type="radio" name={@mode_field.name} id={"scopes-#{@value}-all"} value="all" checked={@private_scope? or @mode_field.value == "all"} disabled={@private_scope?} />
          <label class="form-check-label" for={"scopes-#{@value}-all"}>All {@resource}</label>
        </div>
        <div class="form-check">
          <input class="form-check-input" type="radio" name={@mode_field.name} id={"scopes-#{@value}-selected"} value="selected" checked={not @private_scope? and @mode_field.value == "selected"} disabled={@private_scope?} />
          <label class="form-check-label" for={"scopes-#{@value}-selected"}>Selected {@resource}</label>
        </div>
        <input :if={not @private_scope? and @mode_field.value == "selected"} type="hidden" name={"#{@selected_field.name}[]"} value="" />
        <.combobox :if={not @private_scope? and @mode_field.value == "selected"} id={"scopes-#{@value}"} name={"#{@selected_field.name}[]"} value={@selected_field.value} prompt={"Select #{@resource}..."} options={@options} empty_text={"No #{@resource} found."} multiple={true} />
      </div>
    </div>
    """
  end

  @default_create_form %{
    "description" => "",
    "scopes" => [],
    "scopes_ingest" => [],
    "scopes_ingest_mode" => "all",
    "scopes_query" => [],
    "scopes_query_mode" => "all",
    "scopes_main" => ["ingest"],
    "private" => "false"
  }

  defp source_options(sources) do
    sources
    |> Enum.sort_by(&String.downcase(&1.name))
    |> Enum.map(&{&1.name, "ingest:source:#{&1.id}"})
  end

  defp endpoint_options(endpoints) do
    endpoints
    |> Enum.sort_by(&String.downcase(&1.name))
    |> Enum.map(&{&1.name, "query:endpoint:#{&1.id}"})
  end

  defp coerce_scope_list(value) do
    value
    |> List.wrap()
    |> Enum.filter(&is_non_empty_binary/1)
  end

  defp coerce_scope_fields(data, keys) do
    Enum.reduce(keys, data, fn key, acc ->
      Map.replace_lazy(acc, key, &coerce_scope_list/1)
    end)
  end

  def mount(_params, _session, socket) do
    %{assigns: %{user: user}} = socket
    sources = Sources.list_sources_by_user(user)
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)

    socket =
      socket
      |> assign(:show_create_form, false)
      |> assign(:created_token, nil)
      |> assign(:sources, sources)
      |> assign(:endpoints, endpoints)
      |> assign(scopes_ingest_sources: %{})
      |> assign(scopes_query_endpoints: %{})
      |> assign(create_token_form: @default_create_form)
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

    scopes_main_params = params |> Map.get("scopes_main", []) |> coerce_scope_list()
    scopes_ingest_mode = scope_mode(params, "ingest")
    scopes_query_mode = scope_mode(params, "query")
    scopes_ingest_params = selected_scopes(params, "ingest", scopes_ingest_mode)
    scopes_query_params = selected_scopes(params, "query", scopes_query_mode)
    private_scope? = private_scope?(Map.get(params, "private"))

    scopes_main =
      if scopes_ingest_mode == "selected",
        do: List.delete(scopes_main_params, "ingest"),
        else: scopes_main_params

    scopes_main =
      if scopes_query_mode == "selected",
        do: List.delete(scopes_main, "query"),
        else: scopes_main

    scopes =
      if private_scope?,
        do: ["private"],
        else: scopes_main ++ scopes_ingest_params ++ scopes_query_params

    attrs =
      params
      |> Map.take(["description"])
      |> Map.put("scopes", Enum.join(scopes, " "))

    with nil <-
           selected_scope_error(
             private_scope?,
             scopes_main_params,
             scopes_ingest_mode,
             scopes_ingest_params,
             scopes_query_mode,
             scopes_query_params
           ),
         {:ok, token} <- Auth.create_access_token(user, attrs) do
      socket =
        socket
        |> do_refresh()
        |> assign(:show_create_form, false)
        |> assign(:create_token_form, @default_create_form)
        |> assign(:created_token, token)

      {:noreply, socket}
    else
      message when is_binary(message) ->
        {:noreply, put_flash(socket, :error, message)}

      {:error, %Ecto.Changeset{} = changeset} ->
        message =
          LogflareWeb.Utils.stringify_changeset_errors(changeset, "Could not create access token")

        {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event(
        "update-token-form",
        payload,
        socket
      ) do
    data =
      payload
      |> Map.drop(["_csrf_token", "_target"])
      |> coerce_scope_fields(~w(scopes_main scopes_ingest scopes_query))

    scopes_main = Map.get(data, "scopes_main", [])

    data =
      if "ingest" in scopes_main do
        data
      else
        Map.put(data, "scopes_ingest", [])
      end

    data =
      if "query" in scopes_main do
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

  defp scope_mode(params, scope) do
    Map.get_lazy(params, "scopes_#{scope}_mode", fn ->
      if Map.get(params, "scopes_#{scope}", []) == [], do: "all", else: "selected"
    end)
  end

  defp selected_scopes(params, scope, "selected") do
    params |> Map.get("scopes_#{scope}", []) |> coerce_scope_list()
  end

  defp selected_scopes(_params, _scope, _mode), do: []

  defp selected_scope_error(true, _main, _ingest_mode, _ingest, _query_mode, _query), do: nil

  defp selected_scope_error(false, main, ingest_mode, ingest, query_mode, query) do
    cond do
      "ingest" in main and ingest_mode == "selected" and ingest == [] ->
        "Select at least one source"

      "query" in main and query_mode == "selected" and query == [] ->
        "Select at least one endpoint"

      true ->
        nil
    end
  end

  defp private_scope?(value), do: value in [true, "true"]

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
