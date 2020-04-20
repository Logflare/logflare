defmodule LogflareWeb do
  @moduledoc """
  The entrypoint for defining your web interface, such
  as controllers, views, channels and so on.

  This can be used in your application as:

      use LogflareWeb, :controller
      use LogflareWeb, :view

  The definitions below will be executed for every view,
  controller, etc, so keep them short and clean, focused
  on imports, uses and aliases.

  Do NOT define functions inside the quoted expressions
  below. Instead, define any helper function in modules
  and import those modules here.
  """

  def controller do
    quote do
      use Phoenix.Controller, namespace: LogflareWeb
      import Plug.Conn
      alias LogflareWeb.Router.Helpers, as: Routes
      import LogflareWeb.Gettext

      import Phoenix.LiveView.Controller, only: [live_render: 3]
    end
  end

  def view do
    quote do
      use Phoenix.View,
        root: "lib/logflare_web/templates",
        namespace: LogflareWeb

      # Import convenience functions from controllers
      import Phoenix.Controller, only: [get_flash: 2, view_module: 1]

      import Phoenix.LiveView.Helpers,
        only: [
          live_render: 2,
          live_render: 3,
          live_flash: 2,
          live_patch: 2,
          live_component: 4,
          live_component: 3
        ]

      import Phoenix.LiveView,
        only: [
          push_patch: 2
        ]

      import PhoenixLiveReact, only: [live_react_component: 2, live_react_component: 3]

      # Use all HTML functionality (forms, tags, etc)
      use Phoenix.HTML

      alias LogflareWeb.Router.Helpers, as: Routes
      import LogflareWeb.ErrorHelpers
      import LogflareWeb.Gettext
      alias Logflare.JSON
    end
  end

  def router do
    quote do
      use Phoenix.Router
      import Plug.Conn
      import Phoenix.Controller
    end
  end

  def channel do
    quote do
      use Phoenix.Channel, log_join: false, log_handle_in: false
      import LogflareWeb.Gettext
    end
  end

  @doc """
  When used, dispatch to the appropriate controller/view/etc.
  """
  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
