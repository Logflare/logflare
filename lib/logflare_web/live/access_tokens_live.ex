defmodule LogflareWeb.AccessTokensLive do
  @moduledoc false
  use LogflareWeb, :live_view
  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]
  import LogflareWeb.FormattedTimestampComponent
  require Logger
  alias Logflare.Auth
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias LogflareWeb.AccessTokensLive.Form
  alias LogflareWeb.Utils

  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/accounts/<.subheader_path_link live_patch to={~p"/access-tokens"} team={@team}>access tokens</.subheader_path_link>
        <%= if @live_action == :new do %>
          /new
        <% end %>
      </:path>
      <.subheader_link to="https://docs.logflare.app/concepts/access-tokens/" external={true} text="docs" fa_icon="book" />
    </.subheader>

    <section class="content container mx-auto tw-flex tw-flex-col w-full tw-gap-4">
      <div>
        <.button :if={@live_action == :index} variant="primary" phx-click={JS.patch(~p"/access-tokens/new")}>
          Create access token
        </.button>
      </div>
      <div>
        <p style="white-space: pre-wrap">There are 3 ways of authenticating with the API: in the <code>Authorization</code> header, the <code>X-API-KEY</code> header, or the <code>api_key</code> query parameter.

          The <code>Authorization</code> header method expects the header format <code>Authorization: Bearer your-access-token</code>.
          The <code>X-API-KEY</code> header method expects the header format <code>X-API-KEY: your-access-token</code>.
          The <code>api_key</code> query parameter method expects the search format <code>?api_key=your-access-token</code>.</p>

        <.create_token_form :if={@live_action == :new} form={@create_token_form} sources={@sources} endpoints={@endpoints} />

        <.alert :if={@created_token} variant="success">
          <p>Access token created successfully, copy this token to a safe location. For security purposes, this token will not be shown again.</p>

          <pre class="p-2"><%= @created_token.token %></pre>
          <.clipboard_button text={@created_token.token} />
          <.button variant="secondary" phx-click="dismiss-created-token">
            Dismiss
          </.button>
        </.alert>
      </div>

      <.alert :if={@access_tokens == []} variant="dark" class="tw-max-w-md">
        <h5>Legacy Ingest API Key</h5>
        <p><strong>Deprecated</strong>, use access tokens instead.</p>
        <.clipboard_button text={@user.api_key} class="btn-sm" />
      </.alert>

      <.table :if={@access_tokens != []} id="access-tokens" rows={@access_tokens} row_id={&"access-token-#{&1.id}"}>
        <:col :let={token} label="Description">
          <span :if={is_non_empty_binary(token.description)} class="tw-text-sm">{token.description}</span>
          <span :if={not is_non_empty_binary(token.description)} class="tw-text-sm tw-italic">No description</span>
        </:col>
        <:col :let={token} label="Scope">
          <span :for={label <- scope_labels(assigns, token.scopes)} class="badge badge-secondary mr-1">
            {label}
          </span>
        </:col>
        <:col :let={token} label="Created on">
          <.formatted_timestamp value={token.inserted_at} timezone={@user_timezone} format="%d %b %Y, %I:%M:%S %p" class="tw-text-sm" />
        </:col>
        <:col :let={token} label="Last used">
          <.formatted_timestamp :if={token.usage} value={token.usage.last_used_at} timezone={@user_timezone} format="%d %b %Y, %I:%M:%S %p" class="tw-text-sm" />
          <span :if={is_nil(token.usage)} class="tw-text-sm tw-italic">Unknown</span>
        </:col>
        <:action :let={token}>
          <.clipboard_button :if={!(token.scopes =~ "private")} text={token.token} class="btn-sm" />
        </:action>
        <:action :let={token}>
          <.button class="text-danger btn-sm" data-confirm="Are you sure? This cannot be undone." phx-click="revoke-token" phx-value-token-id={token.id} data-toggle="tooltip" data-placement="top" title="Revoke access token forever">
            <i class="fa fa-trash" aria-hidden="true"></i> Revoke
          </.button>
        </:action>
      </.table>
    </section>
    """
  end

  attr :form, :any, required: true
  attr :sources, :list, required: true
  attr :endpoints, :list, required: true

  defp create_token_form(assigns) do
    ~H"""
    <.form :let={f} for={@form} as={:access_token} action="#" phx-change="update-token-form" phx-submit="create-token" class="mt-4 jumbotron jumbotron-fluid tw-p-4">
      <h5>New Access Token</h5>
      <div class="form-group">
        <label for={f[:description].id}>Description</label>
        <.input field={f[:description]} autofocus />
        <small class="form-text text-muted">A short description for identifying what this access token is to be used for.</small>
      </div>

      <div class="form-group ">
        <label name="scopes" class="tw-mr-3">Scope</label>
        <.inputs_for :let={permission_form} field={f[:ingest]}>
          <.scope value="ingest" label="Ingest" description="Choose whether this token can ingest into all or selected sources." form={permission_form} private_scope?={private_scope?(f[:private].value)} resource="sources" options={resource_options(@sources)} />
        </.inputs_for>
        <.inputs_for :let={permission_form} field={f[:query]}>
          <.scope value="query" label="Query" description="Choose whether this token can query all or selected endpoints." form={permission_form} private_scope?={private_scope?(f[:private].value)} resource="endpoints" options={resource_options(@endpoints)} />
        </.inputs_for>
        <div class="form-check tw-mr-2">
          <.input field={f[:private]} type="checkbox" />
          <label class="form-check-label tw-px-1" for={f[:private].id}>
            Private <small class="form-text text-muted">For account management, has all privileges</small>
          </label>
        </div>
      </div>
      <.button variant="secondary" phx-click={JS.patch(~p"/access-tokens")}>Cancel</.button>
      {submit("Create", class: "btn btn-primary")}
    </.form>
    """
  end

  attr :value, :string, required: true
  attr :label, :string, required: true
  attr :description, :string, required: true
  attr :form, Phoenix.HTML.Form, required: true
  attr :private_scope?, :boolean, required: true
  attr :resource, :string, required: true
  attr :options, :list, required: true

  defp scope(assigns) do
    assigns =
      assigns
      |> assign(:enabled?, enabled?(assigns.form[:enabled].value))
      |> assign(:mode, mode(assigns.form[:mode].value))
      |> assign(:selected_values, selected_values(assigns.form[:selected_ids].value))

    ~H"""
    <div class="form-check tw-mr-2">
      <.input :if={@private_scope?} type="hidden" name={@form[:enabled].name} value={to_string(@enabled?)} />
      <.input field={@form[:enabled]} type="checkbox" id={"scopesmain#{@value}"} checked={@enabled? or @private_scope?} disabled={@private_scope?} />
      <label class="form-check-label tw-px-1" for={["scopes", "main", @value]}>
        {@label}
        <small class="form-text text-muted">{@description}</small>
      </label>
      <.input :if={not @enabled? or @private_scope?} type="hidden" name={@form[:mode].name} value={@mode} />
      <.input :for={selected <- @selected_values} :if={not @enabled? or @private_scope? or @mode != :selected} type="hidden" name={"#{@form[:selected_ids].name}[]"} value={selected} />
      <div :if={@enabled? or @private_scope?} id={"scopes-#{@value}-permissions-#{if(@private_scope?, do: "private", else: "standard")}"} class="tw-ml-5 tw-mt-2 tw-max-w-3xl">
        <div class="form-check">
          <.input field={@form[:mode]} class="form-check-input" type="radio" id={"scopes-#{@value}-all"} value="all" checked={@private_scope? or @mode == :all} disabled={@private_scope?} />
          <label class="form-check-label" for={"scopes-#{@value}-all"}>All {@resource}</label>
        </div>
        <div class="form-check">
          <.input field={@form[:mode]} class="form-check-input" type="radio" id={"scopes-#{@value}-selected"} value="selected" checked={not @private_scope? and @mode == :selected} disabled={@private_scope?} />
          <label class="form-check-label" for={"scopes-#{@value}-selected"}>Selected {@resource}</label>
        </div>
        <.input :if={not @private_scope? and @mode == :selected} type="hidden" name={"#{@form[:selected_ids].name}[]"} value="" />
        <.combobox :if={not @private_scope? and @mode == :selected} id={"scopes-#{@value}"} name={"#{@form[:selected_ids].name}[]"} value={@selected_values} prompt={"Select #{@resource}..."} prompt_hidden={true} options={@options} empty_text={"No #{@resource} found."} multiple={true} />
      </div>
    </div>
    """
  end

  defp enabled?(value), do: value in [true, "true"]

  defp mode(value) when value in [:all, "all"], do: :all
  defp mode(value) when value in [:selected, "selected"], do: :selected
  defp mode(_value), do: nil

  defp selected_values(values), do: Enum.reject(values, &is_nil/1)

  defp resource_options(resources) do
    resources
    |> Enum.sort_by(&String.downcase(&1.name))
    |> Enum.map(&{&1.name, &1.id})
  end

  def mount(_params, _session, socket) do
    %{assigns: %{user: user}} = socket
    sources = Sources.list_sources_by_user(user)
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)
    timezone = socket |> get_connect_params() |> get_in(["user_timezone"])

    socket =
      socket
      |> assign(:user_timezone, timezone)
      |> assign(:created_token, nil)
      |> assign(:sources, sources)
      |> assign(:endpoints, endpoints)
      |> assign(scopes_ingest_sources: %{})
      |> assign(scopes_query_endpoints: %{})
      |> assign(create_token_form: Form.new() |> Form.change())
      |> do_refresh()

    {:ok, socket}
  end

  def handle_params(_params, _uri, socket), do: {:noreply, socket}

  def handle_event("dismiss-created-token", _params, socket) do
    {:noreply, assign(socket, created_token: nil)}
  end

  def handle_event(
        "create-token",
        payload,
        %{assigns: %{user: user}} = socket
      ) do
    params = form_params(payload)

    Logger.debug(
      "Creating access token for user, user_id=#{inspect(user.id)}, params: #{inspect(params)}"
    )

    source_ids = user |> Sources.list_sources_by_user() |> Enum.map(& &1.id)
    endpoint_ids = Endpoints.list_endpoints_by(user_id: user.id) |> Enum.map(& &1.id)
    changeset = Form.validate(Form.new(), params, source_ids, endpoint_ids)

    if changeset.valid? do
      form = Ecto.Changeset.apply_changes(changeset)
      attrs = %{description: form.description, scopes: form |> Form.to_scopes() |> Enum.join(" ")}

      case Auth.create_access_token(user, attrs) do
        {:ok, token} ->
          socket =
            socket
            |> do_refresh()
            |> assign(:create_token_form, Form.new() |> Form.change())
            |> assign(:created_token, token)
            |> push_patch(to: ~p"/access-tokens")

          {:noreply, socket}

        {:error, %Ecto.Changeset{} = changeset} ->
          message = changeset |> Utils.stringify_changeset_errors("Could not create access token")

          {:noreply, put_flash(socket, :error, message)}
      end
    else
      message = changeset |> Utils.stringify_changeset_errors("Could not create access token")

      {:noreply,
       socket
       |> assign(:create_token_form, changeset)
       |> put_flash(:error, message)}
    end
  end

  def handle_event(
        "update-token-form",
        payload,
        socket
      ) do
    form = Ecto.Changeset.apply_changes(socket.assigns.create_token_form)
    changeset = form |> Form.change(form_params(payload)) |> Map.put(:action, :validate)

    {:noreply, assign(socket, :create_token_form, changeset)}
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

  defp form_params(%{
         "access_token" =>
           %{
             "description" => _description,
             "private" => _private,
             "ingest" => _ingest,
             "query" => _query
           } = params
       }),
       do: params

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

  @spec scope_labels(map(), String.t() | nil) :: [String.t()]
  defp scope_labels(assigns, scopes) do
    String.split(scopes || "")
    |> Enum.map(fn
      "ingest" <> _ = scope -> get_ingest_label(assigns, scope)
      "query" <> _ = scope -> get_query_label(assigns, scope)
      scope -> scope
    end)
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
