defmodule Logflare.Sources.Source.RateCounterServerTest do
  @moduledoc false
  alias Logflare.Sources.Source.RateCounterServer
  use LogflareWeb.ConnCase

  test "get_rate/1, get_avg_rate/1 get_max_rate/1, get_rate_metrics/1, get_insert_count/1" do
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({RateCounterServer, source: source})
    assert RateCounterServer.get_rate(source.token) == 0
    assert RateCounterServer.get_avg_rate(source.token) == 0
    assert RateCounterServer.get_max_rate(source.token) == 0
    assert %{sum: 0, duration: 60, average: 0} = RateCounterServer.get_rate_metrics(source.token)
    assert {:ok, 0} = RateCounterServer.get_insert_count(source.token)
  end

  describe "single tenant mode - postgres backend" do
    TestUtils.setup_single_tenant(
      backend_type: :postgres,
      seed_user: true,
      seed_backend: true
    )

    test "can start process without calling Goth" do
      user = insert(:user)
      source = insert(:source, user: user)
      start_link_supervised!({RateCounterServer, source: source})
      :timer.sleep(500)
    end
  end
end
