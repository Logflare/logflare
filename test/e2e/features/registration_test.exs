defmodule E2e.Features.RegistrationTest do
  use Logflare.FeatureCase, async: false

  setup do
    start_supervised!(Logflare.SystemMetricsSup)

    :ok
  end

  describe "single tenant ui" do
    TestUtils.setup_single_tenant(seed_user: true)

    test "redirect to dashboard", %{session: session} do
      session
      |> visit(~p"/")
      |> assert(page_url(&(&1.path == ~p"/dashboard")))
    end
  end
end
