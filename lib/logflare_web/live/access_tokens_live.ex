defmodule LogflareWeb.AccessTokensLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Auth

  def render(assigns) do
    ~H"""
    <div class="subhead">
      <div class="container mx-auto">
        <h5>~/account/access tokens</h5>
      </div>
    </div>

    <section class="content container mx-auto flex flex-col w-full">
      <div class="mb-4">
        <p>
          <strong>Accesss tokens are only supported for Logflare Endpoints for now.</strong>
        </p>
        <p style="white-space: pre-wrap">Theree 3 ways of authenticating with the API: in the <code>Authorization</code> header, the <code>X-API-KEY</code> header, or the <code>api_key</code> query parameter.

          The <code>Authorization</code> header method expects the header format <code>Authorization: Bearer your-access-token</code>.
          The <code>X-API-KEY</code> header method expects the header format <code>X-API-KEY: your-access-token</code>.
          The <code>api_key</code> query parameter method expects the search format <code>?api_key=your-access-token</code>.</p>
        <button class="btn btn-primary" phx-click="toggle-create-form" phx-value-show="true">
          Create access token
        </button>

        <form phx-submit="create-token" class={["mt-4", if(@show_create_form == false, do: "hidden")]}>
          <label>Description</label>
          <input name="description" autofocus />
          <%= submit("Create") %>
          <button type="button" phx-click="toggle-create-form" phx-value-show="false" ->Cancel</button>
        </form>

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
        <p>You do not have any access tokens yet.</p>
      <% end %>

      <table class={["table-dark", "table-auto", "mt-4", "w-full", "flex-grow", if(@access_tokens == [], do: "hidden")]}>
        <thead>
          <tr>
            <th class="p-2">Description</th>
            <th class="p-2">Created on</th>
            <th class="p-2"></th>
          </tr>
        </thead>
        <tbody>
          <%= for token <- @access_tokens do %>
            <tr>
              <td class="p-2">
                <%= token.description %>
              </td>
              <td class="p-2">
                <%= Calendar.strftime(token.inserted_at, "%d %b %Y, %I:%M:%S %p") %>
              </td>
              <td class="p-2">
                <button class="btn text-danger text-bold" data-confirm="Are you sure? This cannot be undone." phx-click="revoke-token" phx-value-token-id={token.id}>
                  Revoke
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
        %{"description" => description} = params,
        %{assigns: %{user: user}} = socket
      ) do
    Logger.debug(
      "Creating access token for user, user_id=#{inspect(user.id)}, params: #{inspect(params)}"
    )

    {:ok, token} = Auth.create_access_token(user, %{description: description})

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
