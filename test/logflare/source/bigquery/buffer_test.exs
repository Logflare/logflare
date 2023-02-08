defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Logs

  use Logflare.DataCase, async: false

  doctest BufferCounter

  setup do
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)

    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    rls = %RecentLogsServer{source: source, source_id: source.token}
    start_supervised!({RecentLogsServer, rls}, id: :source)

    :timer.sleep(250)
    [source: source]
  end

  test "increment buffer counter", %{source: source} do
    batch = [
      %{"event_message" => "any", "metadata" => "some_value"}
    ]

    Logs.ingest_logs(batch, source)

    assert 1 = BufferCounter.get_count(source)
  end
end
