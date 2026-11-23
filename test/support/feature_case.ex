defmodule Logflare.FeatureCase do
  use ExUnit.CaseTemplate

  using opts do
    quote do
      use Playwright.Test, unquote(opts)
      use Fluffy.Assert
      use Mimic

      use LogflareWeb, :routes

      import Fluffy
      import Fluffy.Locator
      import Logflare.Factory
      import unquote(__MODULE__)

      alias Logflare.Backends.IngestEventQueue
      alias Logflare.PubSubRates
      alias Logflare.TestUtils

      require Logflare.TestUtils

      # The default endpoint for testing
      @router LogflareWeb.Router
      @endpoint LogflareWeb.Endpoint
      @moduletag :feature

      setup context do
        Mimic.verify_on_exit!(context)

        on_exit(fn ->
          IngestEventQueue.delete_all_mappings()
          PubSubRates.Cache.clear()
        end)

        %{session: start_session(:playwright)}
      end
    end
  end

  @doc "Logs in as the single-tenant default user, as `/auth/login/single_tenant` does."
  @spec log_in(Fluffy.Session.t()) :: Fluffy.Session.t()
  def log_in(session) do
    user = Logflare.SingleTenant.get_default_user()
    Fluffy.add_session_cookie(session, %{current_email: user.email})
  end

  setup tags do
    Logflare.DataCase.setup_sandbox(tags)
    Logflare.DataCase.setup_mocking(tags)

    :ok
  end
end
