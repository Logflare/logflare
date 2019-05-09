defmodule Logflare.TableRateCounterTest do
  alias Logflare.TableRateCounter, as: TRC
  use ExUnit.Case

  import Mox

  setup :verify_on_exit!

  describe "table rate counter" do
    test "init and handle_info(:put_rate, state)/2" do
      expect(Logflare.TableCounterMock, :get_inserts, fn _ -> {:ok, 10} end)
      table = :some_non_existing_table

      state = %{
        table: table,
        previous_count: 5,
        max_rate: 0,
        begin_time: System.monotonic_time(:second) - 1
      }

      TRC.setup_ets_table(state)

      {:noreply, state} = TRC.handle_info(:put_rate, state)

      assert %{
               table: ^table,
               previous_count: 10,
               current_rate: 5,
               begin_time: _,
               max_rate: 5
             } = state
    end
  end
end
