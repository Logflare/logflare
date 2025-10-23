defmodule LogflareWeb.FeatureCase do
  use ExUnit.CaseTemplate

  using opts do
    quote do
      use PhoenixTest.Playwright.Case, unquote(opts)
      use Mimic

      import Logflare.Factory
      import Logflare.FeatureCase

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
