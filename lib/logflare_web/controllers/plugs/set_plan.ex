defmodule LogflareWeb.Plugs.SetPlan do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.User
  alias Logflare.Billing

  def init(_), do: nil

  def call(conn, opts \\ [])
  def call(%{assigns: %{user: %User{}}} = conn, opts), do: set_plan(conn, opts)

  def call(conn, _opts), do: conn

  defp set_plan(%{assigns: %{user: user}} = conn, _opts) do
    plan = Billing.get_plan_by_user(user)

    conn
    |> assign(:plan, plan)
  end
end
