defmodule LogflareWeb.Plugs.CheckSourceCount do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(%{assigns: %{user: user, plan: plan}} = conn, _params) do
    if user.billing_enabled? do
      source_count = length(user.sources)

      if source_count >= plan.limit_sources do
        conn
        |> put_flash(
          :error,
          "You have #{source_count} sources. Your limit is #{plan.limit_sources}. Delete one or upgrade first!"
        )
        |> redirect(to: Routes.source_path(conn, :dashboard))
        |> halt()
      else
        conn
      end
    else
      conn
    end
  end

  def call(conn, _params), do: conn
end
