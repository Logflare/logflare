defmodule Logflare.SourceRateCounterTest do
  alias Logflare.SourceRateCounter, as: SRC
  import SRC
  use ExUnit.Case

  import Mox

  setup :verify_on_exit!

  setup_all do
    Mox.defmock(Logflare.TableCounterMock, for: Logflare.TableCounter)

    :ok
  end

  setup do
    source_id = :some_non_existing_table

    state = %{}
    {:ok, table_counter_agent} = Agent.start_link(fn -> 0 end)

    SRC.setup_ets_table(source_id)

    {:ok, source_id: source_id, agent: table_counter_agent}
  end

  describe "source_id rate counter" do
    test "init and handle_info(:put_rate, state)/2", %{source_id: source_id} do
      expect(Logflare.TableCounterMock, :get_inserts, fn _ -> {:ok, 10} end)
      {:noreply, sid} = SRC.handle_info(:put_rate, source_id)

      assert sid == source_id

      new_state = get(source_id)

      assert new_state == %Logflare.SourceRateCounter{
               begin_time: new_state.begin_time,
               buckets: %{60 => %{
                 sum: 10,
                 duration: 60,
                 average: 10,
                  queue: new_state.buckets[60].queue
                 }},
               count: 10,
               last_rate: 10,
               max_rate: 10,
               source_id: :some_non_existing_table
             }
    end

    test "get_* functions", %{source_id: source_id} do
      expect(Logflare.TableCounterMock, :get_inserts, 1, fn _table ->
        {:ok, 5}
      end)

      _ = SRC.handle_info(:put_rate, source_id)
      assert SRC.get_rate(source_id) == 5
      assert SRC.get_avg_rate(source_id) == 5
      assert SRC.get_max_rate(source_id) == 5
    end

    test "bucket data is calculated correctly", %{source_id: source_id} do
      state = new(source_id)

      state =
        state
        |> update_state(5)
        |> update_state(50)
        |> update_state(60)

      %{source_id: source_id} = state
      assert state.buckets[60].average == 20
      assert state.max_rate == 45
      assert state.last_rate == 10
    end

    test "get_metrics and get_x functions", %{source_id: source_id} do
      state = new(source_id)

      state = update_state(state, 5)
      update_ets_table(state)

      assert get_rate(source_id) == 5
      assert get_avg_rate(source_id) == 5
      assert get_max_rate(source_id) == 5
      assert get_metrics(source_id) == %{average: 5, sum: 5, duration: 60}

      state = update_state(state, 50)
      update_ets_table(state)

      assert get_rate(source_id) == 45
      assert get_avg_rate(source_id) == 25
      assert get_max_rate(source_id) == 45
      assert get_metrics(source_id) == %{average: 25, sum: 50, duration: 60}

      state = update_state(state, 60)
      update_ets_table(state)

      assert get_rate(source_id) == 10
      assert get_avg_rate(source_id) == 20
      assert get_max_rate(source_id) == 45
      assert get_metrics(source_id) == %{average: 20, sum: 60, duration: 60}
    end
  end
end
