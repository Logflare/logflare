defmodule LogflareWeb.Plugs.VerifyResourceOwnership do
  @moduledoc """
  Plug that checks for ownership of the a provided resource.

  If the `:user` assign is not set, verification is assumed to have passed and as passthroguh is performed.
  If no resource is set, performs a passthrough.
  """
  import Plug.Conn
  alias Logflare.Source
  alias Logflare.Endpoints.Query
  alias Logflare.User
  def init(_opts), do: nil

  # no user set. (handles endpoints with no auth)
  def call(%{assigns: assigns} = conn, _opts) when is_map_key(assigns, :user) == false, do: conn

  # check source
  def call(%{assigns: %{user: %User{id: id}, source: %Source{user_id: user_id}}} = conn, _opts)
      when id == user_id,
      do: conn

  # check endpoint
  def call(%{assigns: %{user: %User{id: id}, endpoint: %Query{user_id: user_id}}} = conn, _opts)
      when id == user_id,
      do: conn

  # halts all others
  def call(%{assigns: assigns} = conn, _)
      when is_map_key(assigns, :source) or is_map_key(assigns, :endpoint) do
    # halt all unmatching
    conn
    |> put_status(401)
    |> halt()
  end

  # no resource is set, passthrough
  def call(conn, _), do: conn
end
