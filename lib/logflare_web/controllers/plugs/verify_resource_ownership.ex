defmodule LogflareWeb.Plugs.VerifyResourceOwnership do
  @moduledoc """
  Plug that checks for ownership of the a provided resource.

  If the `:user` assign is not set, verification is assumed to have passed and as passthroguh is performed.
  If no resource is set, performs a passthrough.
  """
  alias Logflare.Source
  alias Logflare.Endpoints.Query
  alias Logflare.User
  alias LogflareWeb.Api.FallbackController
  def init(_opts), do: nil

  def call(%{assigns: %{endpoint: %Query{enable_auth: false}}} = conn, _opts) do
    conn
  end

  # check source
  def call(%{assigns: %{user: %User{id: id}, source: %Source{user_id: user_id}}} = conn, _opts)
      when id == user_id do
    conn
  end

  # check endpoint
  def call(%{assigns: %{user: %User{id: id}, endpoint: %Query{user_id: user_id}}} = conn, _opts)
      when id == user_id do
    conn
  end

  # halts all others
  def call(%{assigns: assigns} = conn, _) when is_map_key(assigns, :resource_type) do
    FallbackController.call(conn, {:error, :unauthorized})
  end

  # no resource is set, passthrough
  def call(conn, _), do: conn
end
