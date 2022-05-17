defmodule LogflareWeb.AccessTokensLive do
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Auth
  def render(assigns) do
    ~L"""
    <div class="subhead subhead-fixed">
    <div class="container mx-auto">
    <h5>~/account/access tokens</h5>
    </div>
    </div>

    <section class="container mx-auto flex flex-col w-full">
      <div >
        <button class="btn btn-primary" phx-click="create-token">
          Create access token
        </button>
      </div>

      <table class="table-dark table-auto mt-4 w-full flex-grow">
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
                some descr
              </td>
              <td class="p-2">
                <%=  token.inserted_at |> Calendar.strftime("%d %b %Y, %I:%M:%S %p") %>
              </td>
              <td class="p-2">
                <button class="btn text-danger text-bold"
                    data-confirm="Are you sure? This cannot be undone."
                    phx-click="revoke-token"
                    phx-value-token-id="<%= token.id %>">
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


  def mount(_params, %{"user_id"=> user_id} , socket) do
    user = Logflare.Users.get(user_id)

    socket =
      socket
      |> assign(:user, user)
      |> do_refresh()

    {:ok, socket}
  end

  def handle_event("create-token", _unsigned_params, %{assigns: %{user: user}}= socket) do
    Logger.debug("Creating access token for user, user_id=#{inspect(user.id)}")
    {:ok, _token} = Auth.create_access_token(user)
    socket = socket
    |> do_refresh()
    {:noreply, socket}
  end

  def handle_event("revoke-token", %{"token-id"=> token_id}, %{ assigns: %{access_tokens: tokens}} = socket) do
    IO.inspect(token_id)
    token = Enum.find(tokens, &("#{&1.id}" == token_id))
    Logger.debug("Revoking access token")
    :ok  = Auth.revoke_access_token(token)
    socket = socket
      |> do_refresh()
    {:noreply, socket}
  end

  defp do_refresh(%{assigns: %{user: user}}= socket) do
    socket
    |> assign(access_tokens: Auth.list_valid_access_tokens(user))

  end
end
