defmodule LogflareWeb.Hooks.AllowTestSandbox do
  @moduledoc """
  LiveView hook for enabling Ecto SQL Sandbox access during concurrent browser
  testing powered by headless browsers (e.g. ChromeDriver).

  ## Usage

  Register this hook in your router or LiveView definitions:

      defmodule LogflareWeb.Router do
        use LogflareWeb, :router

        live_session :authenticated do
          on_mount(LogflareWeb.Hooks.AllowTestSandbox)
          live "/dashboard", DashboardLive.Index, :index
        end
      end

  Or attach to individual LiveView mounts:

      defmodule MyLive do
        use LogflareWeb, :live_view
        on_mount(LogflareWeb.Hooks.AllowTestSandbox)
      end

  ## See Also

  - [Phoenix Testing Guide](https://hexdocs.pm/phoenix/testing.html)
  - [Concurrent Browser Testing](https://hexdocs.pm/phoenix_ecto/main.html#concurrent-browser-tests)
  - [Ecto.Adapters.SQL.Sandbox](https://hexdocs.pm/ecto_sql/Ecto.Adapters.SQL.Sandbox.html)
  """

  import Phoenix.LiveView
  import Phoenix.Component

  @doc @moduledoc
  @spec on_mount(atom(), map(), map(), Phoenix.LiveView.Socket.t()) ::
          {:cont, Phoenix.LiveView.Socket.t()}
  def on_mount(:default, _params, _session, socket) do
    socket =
      assign_new(socket, :phoenix_ecto_sandbox, fn ->
        if connected?(socket), do: get_connect_info(socket, :user_agent)
      end)

    metadata = socket.assigns.phoenix_ecto_sandbox
    Phoenix.Ecto.SQL.Sandbox.allow(metadata, Ecto.Adapters.SQL.Sandbox)
    {:cont, socket}
  end
end
