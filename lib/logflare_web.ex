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

      import LogflareWeb.Gettext
      import Plug.Conn
      import Phoenix.LiveView.Controller

      unquote(path_helpers())

      # define global controller functions

      @doc """
      plug helper function for controller level assings setting.
      It will set the assigns for each controller action.
      ```
      plug :assign {:banner, @some_value}
      ```
      """
      def assign(conn, {key, value}), do: assign(conn, key, value)
    end
  end

  def view do
    quote do
      alias Logflare.JSON

      use Phoenix.View,
        root: "lib/logflare_web/templates",
        namespace: LogflareWeb

      # Import convenience functions from controllers
      import Phoenix.Controller,
        only: [get_flash: 1, get_flash: 2, view_module: 1, view_template: 1]

      # Use all HTML functionality (forms, tags, etc)
      unquote(view_helpers())
      unquote(path_helpers())
    end
  end

  def live_view_with_templates(params) do
    quote do
      use Phoenix.View,
        root: unquote(params[:root]),
        path: unquote(params[:path]),
        namespace: LogflareWeb

      # Import convenience functions from controllers
      import Phoenix.Controller,
        only: [get_flash: 1, get_flash: 2, view_module: 1, view_template: 1]

      import Phoenix.Component, only: [assign: 2, assign: 3]
      import Phoenix.LiveView, only: [connected?: 1]
      # Use all HTML functionality (forms, tags, etc)
      unquote(view_helpers())
      unquote(live_view_helpers())
      unquote(path_helpers())
    end
  end

  def live_view do
    quote do
      use Phoenix.LiveView,
        layout: {LogflareWeb.LayoutView, :live}

      # declare endpoint and router for Phoenix.VerifiedRoutes
      @endpoint LogflareWeb.Endpoint
      @router LogflareWeb.Router

      import PhoenixLiveReact, only: [live_react_component: 2, live_react_component: 3]
      import LogflareWeb.CoreComponents

      unquote(view_helpers())
      unquote(live_view_helpers())
      unquote(path_helpers())
    end
  end

  def live_component do
    quote do
      use Phoenix.LiveComponent

      unquote(view_helpers())
      unquote(live_view_helpers())
      unquote(path_helpers())
    end
  end

  def html do
    quote do
      unquote(view_helpers())
    end
  end

  defp live_view_helpers do
    quote do
      use LogflareWeb.LiveCommons
      use LogflareWeb.ModalLiveHelpers
    end
  end

  defp path_helpers do
    quote do
      # declare endpoint and router for Phoenix.VerifiedRoutes
      @endpoint LogflareWeb.Endpoint
      @router LogflareWeb.Router
      alias LogflareWeb.Router.Helpers, as: Routes
      import Phoenix.VerifiedRoutes
    end
  end

  defp view_helpers do
    quote do
      use Phoenix.HTML

      import Phoenix.LiveView.Helpers
      import PhoenixLiveReact, only: [live_react_component: 2, live_react_component: 3]
      import Phoenix.View
      import Phoenix.Component

      import LogflareWeb.ErrorHelpers
      import LogflareWeb.Gettext

      alias LogflareWeb.LqlHelpers
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

  defmacro __using__(:live_view_with_templates = which) do
    module_path =
      __CALLER__.module
      |> Module.split()
      |> List.last()
      |> String.trim_trailing("View")
      |> String.downcase()

    apply(__MODULE__, which, [
      [root: "lib/logflare_web/live", path: "#{module_path}_live/templates/"]
    ])
  end

  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
