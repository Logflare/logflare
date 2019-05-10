defmodule Logflare.TableRateCounterTest do
  alias Logflare.TableRateCounter, as: TRC
  import TRC
  use ExUnit.Case

  import Mox

  setup :verify_on_exit!

  setup_all do
    Mox.defmock(Logflare.TableCounterMock, for: Logflare.TableCounter)

    :ok
  end

  setup do
    table = :some_non_existing_table

    state = %TRC{
      table: table,
      count: 0,
      max_rate: 0,
      begin_time: System.monotonic_time(:second) - 1
    }

    {:ok, table_counter_agent} = Agent.start_link(fn -> 0 end)

    TRC.setup_ets_table(state)

    {:ok, state: state, agent: table_counter_agent}
  end

  describe "table rate counter" do
    test "init and handle_info(:put_rate, state)/2", %{state: state} do
      expect(Logflare.TableCounterMock, :get_inserts, fn _ -> {:ok, 10} end)
      table = state.table
      {:noreply, state} = TRC.handle_info(:put_rate, state)

      assert %TRC{
               table: ^table,
               count: 10,
               last_rate: 10,
               begin_time: _,
               max_rate: 10,
               buckets: %{
                 60 => %{
                   queue: queue
                 }
               }
             } = state
    end

    test "get_* functions", %{state: state} do
      expect(Logflare.TableCounterMock, :get_inserts, 1, fn _table ->
        {:ok, 5}
      end)

      _ = TRC.handle_info(:put_rate, state)
      %{table: table} = state
      assert TRC.get_rate(table) == 5
      assert TRC.get_avg_rate(table) == 5
      assert TRC.get_max_rate(table) == 5
    end

    test "bucket data is calculated correctly", %{state: state} do
      state =
        state
        |> update_state(5)
        |> update_state(50)
        |> update_state(60)

      %{table: table} = state
      assert state.buckets[60].average == 20
      assert state.max_rate == 45
      assert state.last_rate == 10
    end

    test "source rate metrics are correctly written into ets table", %{state: state} do
      %{table: table} = state

      state = update_state(state, 5)
      update_ets_table(state)

      assert get_rate(table) == 5
      assert get_avg_rate(table) == 5
      assert get_max_rate(table) == 5

      state = update_state(state, 50)
      update_ets_table(state)

      assert get_rate(table) == 45
      assert get_avg_rate(table) == 25
      assert get_max_rate(table) == 45

      state = update_state(state, 60)
      update_ets_table(state)

      assert get_rate(table) == 10
      assert get_avg_rate(table) == 20
      assert get_max_rate(table) == 45
    end
  end
end
