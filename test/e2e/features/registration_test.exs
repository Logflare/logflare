defmodule E2e.Features.RegistrationTest do
  use Logflare.FeatureCase, async: false

  setup do
    start_supervised!(Logflare.SystemMetricsSup)

    :ok
  end

  describe "single tenant ui" do
    TestUtils.setup_single_tenant(seed_user: true)

    test "redirect to dashboard", %{conn: conn} do
      conn
      |> visit(~p"/")
      |> assert_path(~p"/dashboard")
    end
  end
end
