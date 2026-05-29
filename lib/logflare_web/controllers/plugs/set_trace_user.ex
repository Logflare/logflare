defmodule LogflareWeb.Plugs.SetTraceUser do
  @moduledoc """
  Adds the authenticated user (and team user, when present) to the current
  OpenTelemetry request span so HTTP requests can be attributed to a user.

  Must run after a plug that assigns `:user` on the conn (e.g.
  `LogflareWeb.Plugs.SetTeamContext` for browser requests or
  `LogflareWeb.Plugs.VerifyApiAccess` for API requests). It is a no-op when no
  user is assigned, and safe to call when OpenTelemetry is disabled.
  """
  require OpenTelemetry.Tracer

  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User

  @spec init(keyword()) :: keyword()
  def init(opts), do: opts

  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(%{assigns: %{user: %User{} = user, team_user: %TeamUser{} = team_user}} = conn, _opts) do
    OpenTelemetry.Tracer.set_attributes(%{
      "user.id": user.id,
      "team_user.id": team_user.id
    })

    conn
  end

  def call(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    OpenTelemetry.Tracer.set_attribute(:"user.id", user.id)

    conn
  end

  def call(conn, _opts), do: conn
end
