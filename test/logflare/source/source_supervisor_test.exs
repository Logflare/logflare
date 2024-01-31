defmodule Logflare.Source.SupervisorTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.Sources.BufferCounters
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Source.BigQuery.BufferCounter

  test "able to start supervision tree" do
    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)
    start_supervised!(BufferCounters)
    stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    insert(:plan)

    start_supervised!(Source.Supervisor)
    assert {:ok, :started} = Source.Supervisor.ensure_started(source.token)
    assert Source.Supervisor.lookup(BufferCounter, source.token)
    assert BufferCounter.len(source) == 0
    :timer.sleep(1000)
  end
end
