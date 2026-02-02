defmodule Logflare.FeatureCase do
  use ExUnit.CaseTemplate

  using opts do
    quote do
      use PhoenixTest.Playwright.Case, unquote(opts)
      use Mimic

      use LogflareWeb, :routes

      import Logflare.Factory
      import unquote(__MODULE__)

      alias Logflare.TestUtils

      require Logflare.TestUtils

      # The default endpoint for testing
      @router LogflareWeb.Router
      @endpoint LogflareWeb.Endpoint
      @moduletag :feature

      setup context do
        Mimic.verify_on_exit!(context)

        on_exit(fn ->
          Logflare.Backends.IngestEventQueue.delete_all_mappings()
          Logflare.PubSubRates.Cache.clear()
        end)

        :ok
      end
    end
  end

  setup tags do
    Logflare.DataCase.setup_sandbox(tags)
    Logflare.DataCase.setup_mocking(tags)

    :ok
  end
end
