defmodule Logflare.PhoenixOauth2Provider.Web do
  @moduledoc false

  def view do
    quote do
      use Phoenix.View, root: "lib/logflare_web/templates/phoenix_oauth2_provider"

      # Import convenience functions from controllers
      import Phoenix.Controller, only: [get_csrf_token: 0, get_flash: 2, view_module: 1]

      # Use all HTML functionality (forms, tags, etc)
      use Phoenix.HTML

      alias LogflareWeb.Router.Helpers, as: Routes
      import LogflareWeb.ErrorHelpers
      import LogflareWeb.Gettext
      import Logflare.PhoenixOauth2Provider.ViewHelpers
    end
  end

  @doc """
  When used, dispatch to the appropriate controller/view/etc.
  """
  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
