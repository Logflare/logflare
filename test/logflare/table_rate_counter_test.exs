defmodule Logflare.TableRateCounterTest do
  alias Logflare.TableRateCounter, as: TRC
  use ExUnit.Case

  import Mox

  setup :verify_on_exit!

  setup do
    table = :some_non_existing_table

    state = %{
      table: table,
      previous_count: 0,
      max_rate: 0,
      begin_time: System.monotonic_time(:second) - 1
    }

    TRC.setup_ets_table(state)

    {:ok, state: state}
  end

  describe "table rate counter" do
    test "init and handle_info(:put_rate, state)/2", %{state: state} do
      expect(Logflare.TableCounterMock, :get_inserts, fn _ -> {:ok, 10} end)
      table = state.table
      {:noreply, state} = TRC.handle_info(:put_rate, state)

      assert %{
               table: ^table,
               previous_count: 10,
               current_rate: 10,
               begin_time: _,
               max_rate: 10
             } = state
    end

    test "get_* functions", %{state: state}  do
      expect(Logflare.TableCounterMock, :get_inserts, 1, fn _table ->
        {:ok, 5} end)
      _ = TRC.handle_info(:put_rate, state)
      %{table: table} = state
      assert TRC.get_rate(table) == 5
      assert TRC.get_avg_rate(table) == 5
      assert TRC.get_max_rate(table) == 5
    end
  end
end
