defmodule LogflareWeb.Plugs.SetPlan do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.User
  alias Logflare.Billing

  def init(_), do: nil

  def call(conn, opts \\ [])

  def call(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    assign(conn, :plan, Billing.get_plan_by_user(user))
  end

  def call(conn, _opts), do: conn
end
