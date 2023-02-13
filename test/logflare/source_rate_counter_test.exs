defmodule Logflare.Source.RateCounterServerTest do
  @moduledoc false
  alias Logflare.Source.RateCounterServer, as: RCS
  alias Logflare.Source.RecentLogsServer, as: RLS
  import RCS
  alias Logflare.Sources
  use LogflareWeb.ConnCase
  import Logflare.Factory

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    Sources.Counters.start_link()
    Sources.RateCounters.start_link()
    {:ok, _pid} = RCS.start_link(%RLS{source_id: s1.token})

    {:ok, sources: [s1]}
  end

  describe "RateCounterServer GenServer" do
    @tag :failing
    test "handle_info(:put_rate, state)/2", %{sources: [s1 | _]} do
      Sources.Counters.increment_ets_count(s1.token, 10)
      s1_id = s1.token
      assert {:noreply, ^s1_id} = RCS.handle_info(:put_rate, s1.token)

      new_state = get_data_from_ets(s1.token)

      assert new_state == %RCS{
               begin_time: new_state.begin_time,
               buckets: %{
                 60 => %{
                   sum: 10,
                   duration: 60,
                   average: 10,
                   queue: new_state.buckets[60].queue
                 }
               },
               count: 10,
               last_rate: 10,
               max_rate: 10,
               source_id: s1.token
             }
    end
  end

  describe "RateCounterServer API" do
    @tag :failing
    test "get_* functions", %{sources: [s1 | _]} do
      source_id = s1.token
      Sources.Counters.increment_ets_count(source_id, 5)
      _ = RCS.handle_info(:put_rate, source_id)
      assert RCS.get_rate(source_id) == 5
      assert RCS.get_avg_rate(source_id) == 5
      assert RCS.get_max_rate(source_id) == 5
    end

    @tag :failing
    test "bucket data is calculated correctly", %{sources: [s1 | _]} do
      source_id = s1.token

      state = new(source_id)

      state =
        state
        |> update_state(5)
        |> update_state(50)
        |> update_state(60)

      %{source_id: ^source_id} = state
      assert state.buckets[60].average == 20
      assert state.max_rate == 45
      assert state.last_rate == 10
    end

    @tag :failing
    test "get_metrics and get_x functions", %{sources: [s1 | _]} do
      source_id = s1.token

      state = new(source_id)

      state = update_state(state, 5)
      update_ets_table(state)

      assert get_rate(source_id) == 5
      assert get_avg_rate(source_id) == 5
      assert get_max_rate(source_id) == 5

      state = update_state(state, 50)
      update_ets_table(state)

      assert get_rate(source_id) == 45
      assert get_avg_rate(source_id) == 25
      assert get_max_rate(source_id) == 45

      state = update_state(state, 60)
      update_ets_table(state)

      assert get_rate(source_id) == 10
      assert get_avg_rate(source_id) == 20
      assert get_max_rate(source_id) == 45
    end
  end
end
