defmodule LogflareWeb.Plugs.SetPlanFromCache do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.User
  alias Logflare.Billing

  def init(_), do: nil

  def call(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    %{conn | assigns: Map.put(conn.assigns, :plan, Billing.Cache.get_plan_by_user(user))}
  end

  def call(conn, _opts), do: conn
end
