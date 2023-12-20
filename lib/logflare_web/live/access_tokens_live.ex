defmodule LogflareWeb.AccessTokensLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Auth

  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/accounts/<.subheader_path_link live_patch to={~p"/access-tokens"}>access tokens</.subheader_path_link>
      </:path>
      <.subheader_link to="https://docs.logflare.app/concepts/access-tokens/" text="docs" fa_icon="book" />
    </.subheader>

    <section class="content container mx-auto flex flex-col w-full">
      <div class="mb-4">
        <p style="white-space: pre-wrap">Theree 3 ways of authenticating with the API: in the <code>Authorization</code> header, the <code>X-API-KEY</code> header, or the <code>api_key</code> query parameter.

          The <code>Authorization</code> header method expects the header format <code>Authorization: Bearer your-access-token</code>.
          The <code>X-API-KEY</code> header method expects the header format <code>X-API-KEY: your-access-token</code>.
          The <code>api_key</code> query parameter method expects the search format <code>?api_key=your-access-token</code>.</p>
        <button class="btn btn-primary" phx-click="toggle-create-form" phx-value-show="true">
          Create access token
        </button>

        <.form for={%{}} action="#" phx-submit="create-token" class={["mt-4", if(@show_create_form == false, do: "hidden")]}>
          <div class="form-group">
            <label name="description">Description</label>
            <input name="description" autofocus class="form-control" />
            <small class="form-text text-muted">A short description for identifying what this access token is to be used for.</small>
          </div>

          <div class="form-group ">
            <label name="scopes" class="tw-mr-3">Scope</label>
            <%= for %{value: value, description: description} <- [%{
              value: "public",
              description: "For ingestion and endpoints"
            }, %{
              value: "private",
              description: "For account management"
            }] do %>
              <div class="form-check  form-check-inline tw-mr-2">
                <input class="form-check-input" type="radio" name="scopes" id={["scopes", value]} value={value} checked={value == "public"} />
                <label class="form-check-label tw-px-1" for={["scopes", value]}><%= String.capitalize(value) %></label>
                <small class="form-text text-muted"><%= description %></small>
              </div>
            <% end %>
          </div>
          <button type="button" phx-click="toggle-create-form" phx-value-show="false">Cancel</button>
          <%= submit("Create") %>
        </.form>

        <%= if @created_token do %>
          <div class="mt-4">
            <p>Access token created successfully, copy this token to a safe location. For security purposes, this token will not be shown again.</p>

            <pre class="p-2"><%= @created_token.token %></pre>
            <button phx-click="dismiss-created-token">
              Dismiss
            </button>
          </div>
        <% end %>
      </div>

      <%= if @access_tokens == [] do %>
        <div class="alert alert-dark tw-max-w-md">
          <h5>Legacy Ingest API Key</h5>
          <p><strong>Deprecated</strong>, use access tokens instead.</p>
          <button class="btn btn-secondary btn-sm" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: @user.api_key})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
            <i class="fa fa-clone" aria-hidden="true"></i> Copy
          </button>
        </div>
      <% end %>

      <table class={["table-dark", "table-auto", "mt-4", "w-full", "flex-grow", if(@access_tokens == [], do: "hidden")]}>
        <thead>
          <tr>
            <th class="p-2">Description</th>
            <th class="p-2">Scope</th>
            <th class="p-2">Created on</th>
            <th class="p-2"></th>
          </tr>
        </thead>
        <tbody>
          <%= for token <- @access_tokens do %>
            <tr>
              <td class="p-2">
                <span class="tw-text-sm">
                  <%= token.description %>
                </span>
              </td>
              <td>
                <span :for={scope <- String.split(token.scopes || "")} class="badge badge-secondary"><%= scope %></span>
              </td>
              <td class="p-2 tw-text-sm">
                <%= Calendar.strftime(token.inserted_at, "%d %b %Y, %I:%M:%S %p") %>
              </td>

              <td class="p-2">
                <button :if={token.scopes =~ "public"} class="btn btn-secondary btn-sm" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: token.token})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
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

  def mount(_params, %{"user_id" => user_id}, socket) do
    user = Logflare.Users.get(user_id)

    socket =
      socket
      |> assign(:user, user)
      |> assign(:show_create_form, false)
      |> assign(:created_token, nil)
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

    attrs = Map.take(params, ["description", "scopes"])

    {:ok, token} = Auth.create_access_token(user, attrs)

    socket =
      socket
      |> do_refresh()
      |> assign(:show_create_form, false)
      |> assign(:created_token, token)

    {:noreply, socket}
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

    socket
    |> assign(access_tokens: tokens)
  end
end
