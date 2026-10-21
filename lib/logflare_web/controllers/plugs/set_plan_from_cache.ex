defmodule LogflareWeb.Plugs.SetPlanFromCache do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.Billing
  alias Logflare.User
  alias LogflareWeb.Api.FallbackController

  def init(_), do: nil

  def call(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    case Billing.Cache.get_plan_by_user(user) do
      {:error, :database_unavailable} = err -> FallbackController.call(conn, err)
      plan -> assign(conn, :plan, plan)
    end
  end

  def call(conn, _opts), do: conn
end
