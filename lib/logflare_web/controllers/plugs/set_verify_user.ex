defmodule LogflareWeb.Plugs.SetVerifyUser do
  @moduledoc """
  Verifies user access for browser only.
  """
  import Plug.Conn
  alias Logflare.Users
  alias Logflare.User
  alias Logflare.SingleTenant

  def init(_), do: nil

  # Not sure this will ever be called
  def call(%{assigns: %{user: %User{}}} = conn, _opts), do: conn

  def call(conn, opts),
    do: set_user_for_browser(conn, opts)

  defp set_user_for_browser(conn, _opts) do
    is_single_tenant = SingleTenant.single_tenant?()

    user =
      conn
      |> get_session(:user_id)
      |> maybe_parse_binary_to_int()
      # handle single tenant browser usage, should have no auth required
      |> case do
        nil when is_single_tenant == true ->
          SingleTenant.get_default_user().id

        other ->
          other
      end
      |> fetch_preloaded_user_by_id()

    assign(conn, :user, user)
  end

  defp maybe_parse_binary_to_int(nil), do: nil
  defp maybe_parse_binary_to_int(x) when is_integer(x), do: x

  defp maybe_parse_binary_to_int(x) do
    {int, ""} = Integer.parse(x)
    int
  end

  defp fetch_preloaded_user_by_id(id) when is_integer(id) do
    Users.get_by_and_preload(id: id)
    |> Users.preload_team()
    |> Users.preload_billing_account()
    |> Users.preload_sources()
  end

  defp fetch_preloaded_user_by_id(_id), do: nil
end
